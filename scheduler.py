

import argparse
import json
import logging
import os
import time
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from pymongo import MongoClient

# Configure logging
logging.basicConfig(
    filename="file_conversion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Persistent Task Storage File
TASK_FILE = "scheduled_tasks.json"

# Initialize Scheduler
scheduler = BackgroundScheduler()

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
client = MongoClient(MONGO_URI)
db = client["file_conversion"]
logs_collection = db["logs"]

def log_to_mongodb(task_name, input_path, output_path, status, level="INFO"):
    """Log messages to MongoDB."""
    log_entry = {
        "task_name": task_name,
        "input": input_path,
        "output": output_path,
        "status": status,
        "level": level,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    logs_collection.insert_one(log_entry)

def load_tasks():
    """Load scheduled tasks from file."""
    try:
        with open(TASK_FILE, "r") as f:
            tasks = json.load(f)
            return tasks if isinstance(tasks, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_tasks(tasks):
    """Save scheduled tasks to file."""
    with open(TASK_FILE, "w") as f:
        json.dump(tasks, f, indent=4)

def convert_file(task_name, input_path, output_path):
    """Convert file content to uppercase and save as output format."""
    try:
        with open(input_path, "r") as f:
            content = f.read()
        with open(output_path, "w") as f:
            f.write(content.upper())
        logging.info(f"✅ Converted: '{input_path}' -> '{output_path}'")
        log_to_mongodb(task_name, input_path, output_path, "Converted")
    except Exception as e:
        logging.error(f"❌ Error converting '{input_path}': {e}")
        log_to_mongodb(task_name, input_path, output_path, f"Error: {e}", level="ERROR")

def file_conversion_task(task_name, directory, input_format, output_format):
    """Perform file conversion for all matching files in a directory."""
    if not os.path.exists(directory) or not os.path.isdir(directory):
        logging.error(f"Task '{task_name}' failed: Directory '{directory}' not found.")
        log_to_mongodb(task_name, directory, None, "Directory not found", level="ERROR")
        return

    files_converted = 0
    for filename in os.listdir(directory):
        if filename.endswith(f".{input_format}"):
            input_path = os.path.join(directory, filename)
            output_filename = f"{os.path.splitext(filename)[0]}.{output_format}"
            output_path = os.path.join(directory, output_filename)
            convert_file(task_name, input_path, output_path)
            files_converted += 1
    
    if files_converted == 0:
        log_to_mongodb(task_name, directory, None, f"No files with .{input_format} format found")

def add_task(interval, directory, input_format, output_format):
    """Schedule a new file conversion task."""
    scheduled_tasks = load_tasks()
    task_name = f"task_{interval}_{input_format}_{output_format}"

    if task_name in scheduled_tasks:
        print(f"⚠️ Task '{task_name}' is already scheduled.")
        return

    scheduler.add_job(
        file_conversion_task,
        IntervalTrigger(seconds=interval),
        args=[task_name, directory, input_format, output_format],
        id=task_name,
        replace_existing=True
    )
    
    scheduled_tasks[task_name] = {
        "interval": interval,
        "directory": directory,
        "input_format": input_format,
        "output_format": output_format
    }
    save_tasks(scheduled_tasks)
    log_to_mongodb(task_name, directory, None, f"Task scheduled every {interval} seconds")
    print(f"✅ Task '{task_name}' scheduled every {interval} seconds.")

def remove_task(task_name):
    """Remove a scheduled task."""
    scheduled_tasks = load_tasks()
    if task_name in scheduled_tasks:
        scheduler.remove_job(task_name)
        del scheduled_tasks[task_name]
        save_tasks(scheduled_tasks)
        log_to_mongodb(task_name, None, None, "Task removed")
        print(f"✅ Task '{task_name}' removed.")
    else:
        print(f"⚠️ No task found with name '{task_name}'.")

def list_tasks():
    """List all active scheduled tasks."""
    scheduled_tasks = load_tasks()
    if not scheduled_tasks:
        print("📌 No tasks scheduled.")
    else:
        print("📌 Scheduled tasks:")
        for task_name, details in scheduled_tasks.items():
            directory = details.get("directory", "N/A")
            print(f" - {task_name}: Every {details['interval']}s | Dir: {directory} | Input: .{details['input_format']} | Output: .{details['output_format']}")

def load_and_schedule_tasks():
    """Load tasks from storage and schedule them."""
    scheduled_tasks = load_tasks()
    for task_name, details in scheduled_tasks.items():
        if not all(k in details for k in ["interval", "directory", "input_format", "output_format"]):
            print(f"⚠️ Skipping task '{task_name}': Missing required fields.")
            continue

        scheduler.add_job(
            file_conversion_task,
            IntervalTrigger(seconds=details["interval"]),
            args=[
                task_name,
                details.get("directory", "N/A"),
                details.get("input_format", "txt"),
                details.get("output_format", "csv")
            ],
            id=task_name,
            replace_existing=True
        )

def start_scheduler():
    """Runs the scheduler in a separate thread."""
    scheduler.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Scheduler stopped.")
        scheduler.shutdown()

# CLI Argument Parsing
parser = argparse.ArgumentParser(description="Background File Conversion Scheduler")
parser.add_argument("--add", type=int, help="Add a new task with interval (in seconds)")
parser.add_argument("--dir", type=str, help="Directory to scan for files")
parser.add_argument("--input-format", type=str, help="Input file format (e.g., txt)")
parser.add_argument("--output-format", type=str, help="Output file format (e.g., csv)")
parser.add_argument("--list", action="store_true", help="List all scheduled tasks")
parser.add_argument("--remove", type=str, help="Remove a scheduled task by name")

args = parser.parse_args()

# Load and schedule tasks before parsing commands
load_and_schedule_tasks()

if args.add:
    if not args.dir or not args.input_format or not args.output_format:
        print("⚠️ Please provide --dir, --input-format, and --output-format.")
        exit(1)
    add_task(args.add, args.dir, args.input_format, args.output_format)
    exit(0)

elif args.list:
    list_tasks()
    exit(0)

elif args.remove:
    remove_task(args.remove)
    exit(0)

# Start the scheduler thread only if no other command is given
scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
scheduler_thread.start()

# Keep the main thread alive
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("🛑 Scheduler stopped.")
    scheduler.shutdown()

