# celery_app.py
import os
from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_ready

# Allow override via env for local dev
BROKER_URL  = os.getenv(
    "CELERY_BROKER_URL",
    "redis://default:FY0eHpAwCj2eRxoTiUcJTn4T8dkmLWGE@redis-14436.c114.us-east-1-4.ec2.redns.redis-cloud.com:14436/0"
)
BACKEND_URL = os.getenv("CELERY_RESULT_BACKEND", BROKER_URL)

celery = Celery(
    "cti",
    broker=BROKER_URL,
    backend=BACKEND_URL,
    include=["worker.tasks"],   # ✅ 确保注册到 worker.tasks.* 的任务都能被发现
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
        # 每10分钟：主抓取 + 推荐
        "run-fetch-every-10min": {
            "task": "worker.tasks.run_fetch_and_reco",
            "schedule": 600.0,
        },

        # 每天 03:00：重建 CyBOK 推荐
        "run-reco-gridfs-3am": {
            "task": "worker.tasks.run_cybok_reco_gridfs",
            "schedule": crontab(minute=0, hour=3),
        },

        # ✅ 新增：每小时第 20 分，去重抓取仓库里所有 RSS
        "run-fetch-all-rss-hourly": {
            "task": "worker.tasks.run_fetch_all_rss_dedup",
            "schedule": crontab(minute=20),   # 每小时 xx:20
            "kwargs": {
                "limit": 200,
                # "owner_filter": ["alice","bob"],  # 可选
                # "sample": 50,                     # 可选：仅处理前 N 个 URL
            },
        },
    }
else:
    celery.conf.beat_schedule = {}


@worker_ready.connect
def _kickoff_once(sender, **kwargs):
    """
    worker 启动时“只跑一次”的任务。
    若不想自动跑，把环境变量 RUN_STARTUP_TASKS=0
    """
    if os.getenv("RUN_STARTUP_TASKS", "1") != "1":
        return
    app = sender.app
    app.send_task("worker.tasks.run_fetch")
    app.send_task("worker.tasks.run_ingest_cybok_intro_pdf")  # 若没有实现，可保留我们在 tasks.py 里的占位
    app.send_task("worker.tasks.run_cybok_reco_gridfs")
