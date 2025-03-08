from pymongo import MongoClient
import time
from backend.config import MONGO_URI

client = MongoClient(MONGO_URI)
db = client["file_tasks"]
logs_collection = db["logs"]

def log_to_mongodb(task_name, directory, output_path, status, level="INFO"):
    """Log messages to MongoDB."""
    log_entry = {
        "task_name": task_name,
        "directory": directory,
        "output": output_path,
        "status": status,
        "level": level,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    logs_collection.insert_one(log_entry)

def get_logs():
    logs = list(logs_collection.find({}, {'_id': 0}).sort("timestamp", -1))
    return logs