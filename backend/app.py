import logging
import os

log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
import sys
# print(sys.path)
from backend.config import FLASK_SECRET_KEY
from backend.web import app
import threading
from backend.job_scheduler.scheduler import load_and_schedule_tasks, start_scheduler

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, "app.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

app.secret_key = FLASK_SECRET_KEY

if __name__ == "__main__":
    load_and_schedule_tasks()
    scheduler_thread = threading.Thread(target=start_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    app.run(debug=False)