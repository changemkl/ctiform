# ctiapp/tasks_api.py
from flask import Blueprint, jsonify
from celery.result import AsyncResult
from worker.tasks import run_fetch_and_reco
from worker.celery_app import celery
from .utils import login_required

bp = Blueprint("tasks_api", __name__)

@bp.post("/fetch_now")
@login_required
def fetch_now():
    ar = run_fetch_and_reco.delay()
    return jsonify({"task_id": ar.id, "state": ar.state})

@bp.get("/task_status/<task_id>")
@login_required
def task_status(task_id):
    ar = AsyncResult(task_id, app=celery)
    payload = {"task_id": task_id, "state": ar.state}
    if ar.state == "PENDING":
        payload["meta"] = None
    elif ar.state in {"RECEIVED", "STARTED", "PROGRESS"}:
        payload["meta"] = _safe_info(ar.info)
    elif ar.state == "FAILURE":
        payload["meta"] = _safe_info(ar.info)
        payload["traceback"] = ar.traceback
    elif ar.state == "SUCCESS":
        payload["result"] = _safe_info(ar.result)
    return jsonify(payload)

def _safe_info(val):
    if isinstance(val, Exception):
        return {"error": str(val)}
    if isinstance(val, (dict, list, str, int, float, bool)) or val is None:
        return val
    return {"repr": repr(val)}
