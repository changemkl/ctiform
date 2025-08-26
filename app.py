# app.py
import subprocess, sys
from .ctiapp import create_app

app = create_app()

if __name__ == "__main__":
    celery_proc = subprocess.Popen(
        [sys.executable, "-m", "celery", "-A", "worker.tasks", "worker", "-l", "info", "--pool=solo"]
    )
    try:
        app.run(debug=True)
    finally:
        celery_proc.terminate()
