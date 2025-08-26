# worker/tasks.py
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from contextlib import contextmanager

from .celery_app import celery  # ✅ 用绝对导入，避免相对导入在不同入口下失败

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ---------- 跨进程文件锁：仅在“抓取阶段”串行，避免并发导入/初始化死锁 ----------
LOCK_PATH = Path(os.getenv("CTI_FETCH_LOCK_FILE", os.path.join(os.getenv("TMP", os.getenv("TEMP", "/tmp")), "cti_fetch.lock")))

@contextmanager
def fetch_lock():
    import time
    while True:
        try:
            fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_RDWR)
            os.write(fd, str(os.getpid()).encode("utf-8"))
            os.close(fd)
            break
        except FileExistsError:
            time.sleep(0.2)
    try:
        yield
    finally:
        try:
            os.remove(LOCK_PATH)
        except FileNotFoundError:
            pass


# ---------- 若队列里仍有历史任务名，给一个占位，避免“未注册任务” ----------
@celery.task(name="worker.tasks.run_ingest_cybok_intro_pdf")
def run_ingest_cybok_intro_pdf():
    logger.info("run_ingest_cybok_intro_pdf placeholder executed")
    return {"ok": True}


# ---------- 基础抓取任务：运行 task_fetch.main() ----------
@celery.task(name="worker.tasks.run_fetch")
def run_fetch():
    """
    Celery 任务：运行 task_fetch.main()
    """
    try:
        # 延迟导入，避免并发导入链
        from task_fetch import main as _fetch_main
    except ImportError:
        from worker.task_fetch import main as _fetch_main  # 兼容你把文件放在 worker/ 下的情况

    try:
        with fetch_lock():
            _fetch_main()
        return {"ok": True}
    except Exception as e:
        logger.exception("run_fetch failed: %s", e)
        return {"ok": False, "error": str(e)}


# ---------- 用户定向抓取：单个 owner + 单个 RSS ----------
@celery.task(bind=True, name="worker.tasks.run_fetch_user_rss_once")
def run_fetch_user_rss_once(self, owner_username: str, rss_url: str, limit: int = 200):
    """
    仅抓取指定用户/指定 RSS 一次。抓取阶段加锁，避免并发死锁。
    """
    try:
        # 延迟导入
        from task_fetch import fetch_user_rss_once as _fetch_user_rss_once
    except ImportError:
        from worker.task_fetch import fetch_user_rss_once as _fetch_user_rss_once

    self.update_state(state="PROGRESS", meta={"step": "start"})
    try:
        self.update_state(state="PROGRESS", meta={"step": "fetch"})
        with fetch_lock():
            result = _fetch_user_rss_once(owner_username=owner_username, rss_url=rss_url, limit=limit)

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
        logger.exception("run_fetch_user_rss_once failed")
        self.update_state(state="FAILURE", meta={"step": "error", "error": str(e)})
        raise


# ---------- 去重抓取所有 RSS ----------
@celery.task(name="worker.tasks.run_fetch_all_rss_dedup")
def run_fetch_all_rss_dedup(limit: int = 200, owner_filter=None, sample: int | None = None):
    """
    从 user_rss_sources 读取所有 (owner, url)，对 URL 去重后逐一调用 fetch_user_rss_once。
    """
    try:
        from task_fetch import fetch_all_rss_dedup as _fetch_all
    except ImportError:
        from worker.task_fetch import fetch_all_rss_dedup as _fetch_all

    # 这里的函数内部已自行加锁，仅调用即可
    return _fetch_all(limit=limit, owner_filter=owner_filter, sample=sample)


# ---------- 抓取+推荐 的流水线 ----------
@celery.task(bind=True, name="worker.tasks.run_fetch_and_reco")
def run_fetch_and_reco(self):
    """
    先抓取再推荐。抓取阶段与其它抓取任务互斥，推荐阶段不加锁。
    """
    try:
        logger.info("[Celery] run_fetch_and_reco: start")
        self.update_state(state="PROGRESS", meta={"step": "fetch"})

        try:
            from task_fetch import main as _fetch_main
        except ImportError:
            from worker.task_fetch import main as _fetch_main

        with fetch_lock():
            _fetch_main()

        self.update_state(state="PROGRESS", meta={"step": "reco"})
        try:
            from task_cybok_reco_gridfs import main as reco_main
        except ImportError:
            from worker.task_cybok_reco_gridfs import main as reco_main

        reco_main()

        logger.info("[Celery] run_fetch_and_reco: done")
        return {"ok": True, "step": "done"}
    except Exception as e:
        logger.exception("run_fetch_and_reco failed: %s", e)
        self.update_state(state="FAILURE", meta={"step": "error", "err": str(e)})
        raise


# ---------- 仅推荐 ----------
@celery.task(name="worker.tasks.run_cybok_reco_gridfs")
def run_cybok_reco_gridfs():
    try:
        from task_cybok_reco_gridfs import main as reco_main
    except ImportError:
        from worker.task_cybok_reco_gridfs import main as reco_main
    return reco_main()
