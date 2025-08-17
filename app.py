# WORKERS/app.py
import os, sys, subprocess, atexit, time
from flask import Flask, jsonify
from .worker.celery_app import celery
from celery.result import AsyncResult

app = Flask(__name__)

# Keep handles to background processes so we can clean them up on exit
_CHILD_PROCS: list[subprocess.Popen] = []

def _spawn(*args: str):
    """Spawn a background process, inherit env, keep handle for cleanup."""
    env = os.environ.copy()
    p = subprocess.Popen(list(args), env=env)
    _CHILD_PROCS.append(p)
    return p

def start_celery_services():
    """
    Start a Celery worker and Celery beat (scheduler) as background processes.
    Run with solo pool for Windows friendliness.
    """
    # Worker
    _spawn(
        sys.executable, "-m", "celery",
        "-A", "worker.celery_app:celery",
        "worker",
        "--loglevel=info",
        "--pool=solo",
        "--concurrency=1",
        "--max-tasks-per-child=20",
    )
    # Beat (optional, comment out if you don't use periodic tasks)
    _spawn(
        sys.executable, "-m", "celery",
        "-A", "worker.celery_app:celery",
        "beat",
        "--loglevel=info",
    )

def enqueue_startup_tasks():
    """
    Kick off tasks at startup (non-blocking). Adjust names to your tasks.
    """
    try:
        # Examples: these task names must exist in worker/tasks.py
        celery.send_task("worker.run_fetch")            # runs task_fetch.py
        # celery.send_task("worker.run_fetch_and_reco") # if you have it
        # celery.send_task("worker.run_cybok_reco_gridfs")
    except Exception as e:
        # Don't crash the app if queueing fails
        print(f"[startup] failed to enqueue: {e}", flush=True)

@atexit.register
def _cleanup_children():
    for p in _CHILD_PROCS:
        try:
            p.terminate()
        except Exception:
            pass

@app.get("/")
def index():
    return "Flask running; Celery worker/beat auto-started; startup tasks queued."

if __name__ == "__main__":
    start_celery_services()
    time.sleep(1.0)
    enqueue_startup_tasks()
    app.run(host="0.0.0.0", port=8080, debug=False, use_reloader=False)
