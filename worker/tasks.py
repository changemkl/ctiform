from .celery_app import celery
import subprocess, sys
from pathlib import Path
import shlex
# worker/tasks.py
import os, time
from celery.signals import worker_ready
import redis

BASE = Path(__file__).resolve().parents[1]

def _run(pyfile: str, args: list[str] | None = None):
    cmd = [sys.executable, str(BASE / pyfile), *(args or [])]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"[{pyfile}] failed({p.returncode}): {p.stderr.strip()}")
    return {"cmd": " ".join(shlex.quote(c) for c in cmd), "stdout": p.stdout.strip()}

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

@celery.task(bind=True)
def run_fetch_and_reco(self, name="worker.run_fetch_and_reco"):
    self.update_state(state="PROGRESS", meta={"step": "fetch", "percent": 20})
    res1 = _run("task_fetch.py")

    self.update_state(state="PROGRESS", meta={"step": "reco", "percent": 70})
    res2 = _run("task_cybok_reco_gridfs.py")

    return {"fetch": res1, "reco": res2}



def _should_kick_once(ttl_seconds=300) -> bool:
    url = os.getenv("REDIS_URL", "redis://default:FY0eHpAwCj2eRxoTiUcJTn4T8dkmLWGE@redis-14436.c114.us-east-1-4.ec2.redns.redis-cloud.com:14436/0")
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
        else:
            pass
    except Exception as e:
        print(f"[worker_ready] kickoff failed: {e}")
