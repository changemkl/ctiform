# worker/tasks.py
from .celery_app import celery
import subprocess, sys
from pathlib import Path
import shlex
import os, time
from typing import Optional
from celery.signals import worker_ready
import redis
from hashlib import sha1
# External deps for RSS + DB
import feedparser
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timezone

BASE = Path(__file__).resolve().parents[1]

# ----------------------------
# Generic subprocess runner (for your existing scripts)
# ----------------------------
def _run(pyfile: str, args: Optional[list[str]] = None):
    cmd = [sys.executable, str(BASE / pyfile), *(args or [])]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"[{pyfile}] failed({p.returncode}): {p.stderr.strip()}")
    return {"cmd": " ".join(shlex.quote(c) for c in cmd), "stdout": p.stdout.strip()}

# ----------------------------
# DB helpers
# ----------------------------
_MONGO_CLIENT = None

def _get_db():
    global _MONGO_CLIENT
    if _MONGO_CLIENT is None:
        MONGODB_URI = os.getenv(
            "MONGODB_URI",
            "mongodb+srv://yzhang850:a237342160@cluster0.cficuai.mongodb.net/?retryWrites=true&w=majority&authSource=admin",
        )
        _MONGO_CLIENT = MongoClient(MONGODB_URI)
    DB_NAME = os.getenv("DB_NAME", "cti_platform")
    return _MONGO_CLIENT[DB_NAME]

# ----------------------------
# Existing tasks running external scripts
# ----------------------------
@celery.task(bind=True)
def run_ingest_cybok_intro_pdf(self):
    self.update_state(state="PROGRESS", meta={"step": "start", "percent": 10})
    return _run("ingest_cybok_intro_pdf.py")

@celery.task(bind=True)
def run_cybok_reco_gridfs(self):
    self.update_state(state="PROGRESS", meta={"step": "encoding", "percent": 50})
    return _run("task_cybok_reco_gridfs.py")

@celery.task(bind=True)
def run_fetch(self):
    self.update_state(state="PROGRESS", meta={"step": "fetching", "percent": 30})
    return _run("task_fetch.py")

@celery.task(bind=True, name="worker.run_fetch_and_reco")
def run_fetch_and_reco(self):
    self.update_state(state="PROGRESS", meta={"step": "fetch", "percent": 20})
    res1 = _run("task_fetch.py")

    self.update_state(state="PROGRESS", meta={"step": "reco", "percent": 70})
    res2 = _run("task_cybok_reco_gridfs.py")

    return {"fetch": res1, "reco": res2}

# ----------------------------
# User-bound RSS fetch task
# ----------------------------
def _parse_time_struct(entry) -> Optional[datetime]:
    try:
        if getattr(entry, "published_parsed", None):
            return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
    except Exception:
        pass
    try:
        if getattr(entry, "updated_parsed", None):
            return datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
    except Exception:
        pass
    return None

@celery.task(bind=True, name="worker.run_fetch_user_rss_once")
def run_fetch_user_rss_once(self, owner_id: str, rss_url: str, max_items: int = 40):
    """
    Fetch one RSS feed for a specific user and upsert into `threats` with owner-binding.

    - owner_id: MongoDB ObjectId string of the user who owns this subscription
    - rss_url:  RSS/Atom feed URL
    - max_items: cap to avoid overloading on first import
    """
    self.update_state(state="STARTED", meta={"step": "fetching", "url": rss_url})

    db = _get_db()
    threats = db["threats"]
    owner = ObjectId(owner_id)

    d = feedparser.parse(rss_url)


    count = 0
    total = 0
    now = datetime.now(timezone.utc)

    for entry in d.entries[: max(1, int(max_items))]:
        total += 1
        link = entry.get("link") or entry.get("id") or ""
        if not link:
            continue

        title = (entry.get("title") or "").strip() or link
        summary = (entry.get("summary") or "").strip()
        published = _parse_time_struct(entry) or now

        # Upsert per-user by (owner, link)
        sid = f"user:{owner_id}:{sha1(link.encode('utf-8')).hexdigest()}"  # unique per owner+link
        threats.update_one(
            {"source_id": sid},
            {
                "$setOnInsert": {"source_id": sid, "source": "user"},
                "$set": {
                    "owner": owner,
                    "source_url": rss_url,
                    "link": link,
                    "url": link,
                    "title": title,
                    "content": summary,
                    "timestamp": published,
                    "updated_at": now,
                    "min_role": "public",
                    "allowed_roles": ["public", "pro", "admin"],
                },
            },
            upsert=True,
        )
        count += 1

    return {"inserted": count, "total": total, "url": rss_url}

# ----------------------------
# One-time kickoff when worker is ready
# ----------------------------
def _should_kick_once(ttl_seconds=300) -> bool:
    url = os.getenv(
        "REDIS_URL",
        "redis://default:FY0eHpAwCj2eRxoTiUcJTn4T8dkmLWGE@redis-14436.c114.us-east-1-4.ec2.redns.redis-cloud.com:14436/0",
    )
    r = redis.from_url(url)
    key = "once:kickoff:run_fetch_and_reco"
    ok = r.setnx(key, int(time.time()))
    if ok:
        r.expire(key, ttl_seconds)
    return ok

@worker_ready.connect
def _kickoff_on_worker_ready(sender, **kwargs):
    try:
        if _should_kick_once(ttl_seconds=300):
            sender.app.send_task("worker.run_fetch_and_reco")
    except Exception as e:
        print(f"[worker_ready] kickoff failed: {e}")
