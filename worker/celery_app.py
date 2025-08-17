# celery_app.py
import os
from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_ready  # NEW

# Allow override via env for local dev
BROKER_URL  = os.getenv("CELERY_BROKER_URL",
    "redis://default:FY0eHpAwCj2eRxoTiUcJTn4T8dkmLWGE@redis-14436.c114.us-east-1-4.ec2.redns.redis-cloud.com:14436/0")
BACKEND_URL = os.getenv("CELERY_RESULT_BACKEND", BROKER_URL)

celery = Celery(
    "cti",
    broker=BROKER_URL,
    backend=BACKEND_URL,
    include=["worker.tasks"],   # ensure tasks are discovered on import
)

celery.conf.update(
    timezone="Europe/London",
    enable_utc=True,
    task_acks_late=True,
    worker_max_tasks_per_child=20,
    broker_connection_retry_on_startup=True,
    task_time_limit=60 * 45,
    task_soft_time_limit=60 * 40,
)

if os.getenv("DISABLE_BEAT", "0") != "1":
    celery.conf.beat_schedule = {
        "run-fetch-every-10min": {
            "task": "worker.tasks.run_fetch_and_reco",
            "schedule": 600.0,  # 或 crontab(minute="*/10")
        },
        "run-reco-gridfs-3am": {
            "task": "worker.tasks.run_cybok_reco_gridfs",
            "schedule": crontab(minute=0, hour=3),
        },
    }
else:
    celery.conf.beat_schedule = {}


@worker_ready.connect
def _kickoff_once(sender, **kwargs):
    if os.getenv("RUN_STARTUP_TASKS", "1") != "1":
        return
    app = sender.app
    app.send_task("worker.tasks.run_fetch")
    app.send_task("worker.tasks.run_ingest_cybok_intro_pdf")
    app.send_task("worker.tasks.run_cybok_reco_gridfs")
