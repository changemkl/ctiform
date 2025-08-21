# worker/task_fetch.py
import os
import re
import time
from datetime import datetime, timezone
from typing import Tuple, Optional

import requests
from pymongo import MongoClient

# Optional content extraction
try:
    from readability import Document
except Exception:
    Document = None
try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None
try:
    import feedparser
except Exception as e:
    feedparser = None

# -------- Environment --------
MONGODB_URI = os.getenv(
    "MONGODB_URI",
    "mongodb+srv://yzhang850:a237342160@cluster0.cficuai.mongodb.net/?retryWrites=true&w=majority&authSource=admin"
)
DB_NAME = os.getenv("DB_NAME", "cti_platform")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))

mongo = MongoClient(MONGODB_URI)
db = mongo[DB_NAME]
user_rss_sources = db["user_rss_sources"]
user_rss_items = db["user_rss_items"]

UA = "cti-portal/1.0 (+rss)"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})


def _strip_html(html: str) -> str:
    if not html:
        return ""
    txt = re.sub(r"<[^>]+>", " ", html)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _extract_main_content(html: str) -> Tuple[str, str]:
    title, text = "", ""
    if html:
        if Document:
            try:
                doc = Document(html)
                title = (doc.short_title() or "").strip()
                text = _strip_html(doc.summary() or "")
            except Exception:
                pass
        if not text and BeautifulSoup:
            try:
                soup = BeautifulSoup(html, "html.parser")
                article = soup.find("article") or soup
                paras = [p.get_text(" ", strip=True) for p in article.find_all("p")]
                text = " ".join(paras).strip()
                if not title and soup.title and soup.title.string:
                    title = soup.title.string.strip()
            except Exception:
                pass
        if not text:
            text = _strip_html(html)
    return title, text


def _fetch_url(url: str) -> Optional[str]:
    try:
        r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception:
        return None


def _entry_time(entry) -> datetime:
    # Prefer published, then updated, else now
    for key in ("published_parsed", "updated_parsed"):
        t = getattr(entry, key, None) or entry.get(key)
        if t:
            try:
                return datetime.fromtimestamp(time.mktime(t), tz=timezone.utc)
            except Exception:
                pass
    return datetime.now(timezone.utc)


def _normalize_link(entry) -> Optional[str]:
    # Prefer link, then id if it looks like a URL
    url = entry.get("link") or entry.get("id") or ""
    if isinstance(url, list):
        url = url[0] if url else ""
    if isinstance(url, dict):
        url = url.get("href") or ""
    if not url:
        return None
    if not re.match(r"^https?://", url, re.I):
        return None
    return url.strip()


def _upsert_item(owner_username: str, feed_url: str, url: str, title: str, content: str, ts: datetime):
    # Deduplicate per owner+url
    existing = user_rss_items.find_one({"owner_username": owner_username, "url": url})
    if existing:
        user_rss_items.update_one(
            {"_id": existing["_id"]},
            {"$set": {
                "title": title or existing.get("title"),
                "content": content or existing.get("content"),
                "timestamp": ts or existing.get("timestamp"),
                "feed_url": feed_url,
                "updated_at": datetime.now(timezone.utc),
            }}
        )
        return False
    user_rss_items.insert_one({
        "owner_username": owner_username,
        "feed_url": feed_url,
        "url": url,
        "title": title or url,
        "content": content or "",
        "timestamp": ts,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    })
    return True


def fetch_user_rss_once(owner_username: str, rss_url: str, limit: int = 200) -> dict:
    # Parse feed
    if not feedparser:
        raise RuntimeError("feedparser is not installed")
    parsed = feedparser.parse(rss_url)
    status = getattr(parsed, "status", None) or parsed.get("status")
    if status and int(status) >= 400:
        user_rss_sources.update_one(
            {"owner_username": owner_username, "url": rss_url},
            {"$set": {
                "last_status": f"http {status}",
                "last_crawled": datetime.now(timezone.utc)
            }},
            upsert=True
        )
        return {"ok": False, "new": 0, "total": 0, "status": status}

    entries = parsed.entries or []
    total = 0
    new_count = 0

    for entry in entries[:limit]:
        url = _normalize_link(entry)
        if not url:
            continue

        title = (entry.get("title") or "").strip()
        summary = _strip_html(entry.get("summary") or entry.get("description") or "")

        content_text = summary
        if not content_text:
            html = _fetch_url(url)
            if html:
                t2, txt2 = _extract_main_content(html)
                if not title and t2:
                    title = t2
                content_text = txt2

        ts = _entry_time(entry)

        inserted = _upsert_item(
            owner_username=owner_username,
            feed_url=rss_url,
            url=url,
            title=title,
            content=content_text,
            ts=ts
        )
        total += 1
        if inserted:
            new_count += 1

    user_rss_sources.update_one(
        {"owner_username": owner_username, "url": rss_url},
        {"$set": {
            "last_crawled": datetime.now(timezone.utc),
            "last_status": f"ok: {new_count} new / {total} scanned"
        }},
        upsert=True
    )
    return {"ok": True, "new": new_count, "total": total, "status": "ok"}
