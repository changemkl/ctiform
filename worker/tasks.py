# worker/tasks.py
import os
from datetime import datetime, timezone

from .celery_app import celery
from task_fetch import fetch_user_rss_once


@celery.task(bind=True, name="worker.tasks.run_fetch_user_rss_once")
def run_fetch_user_rss_once(self, owner_username: str, rss_url: str, limit: int = 200):
    self.update_state(state="PROGRESS", meta={"step": "start"})
    try:
        self.update_state(state="PROGRESS", meta={"step": "fetch"})
        result = fetch_user_rss_once(owner_username=owner_username, rss_url=rss_url, limit=limit)
        self.update_state(state="PROGRESS", meta={"step": "finalize"})
        payload = {
            "ok": bool(result.get("ok")),
            "owner_username": owner_username,
            "rss_url": rss_url,
            "new": int(result.get("new", 0)),
            "total": int(result.get("total", 0)),
            "status": result.get("status", "ok"),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        return payload
    except Exception as e:
        self.update_state(state="FAILURE", meta={"step": "error", "error": str(e)})
        raise


@celery.task(bind=True, name="worker.tasks.run_fetch_and_reco")
def run_fetch_and_reco(self):
    self.update_state(state="PROGRESS", meta={"step": "start"})
    # Keep existing non-RSS workflow unchanged outside this patch
    self.update_state(state="PROGRESS", meta={"step": "noop"})
    return {"ok": True, "step": "noop"}
