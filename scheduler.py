import argparse
import json
import logging
import os
import time
import threading
import zipfile
import tarfile
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from pymongo import MongoClient
from docx import Document
import pandas as pd
from fpdf import FPDF

# Configure logging
logging.basicConfig(
    filename="file_tasks.log",
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

def convert_file(task_name, input_path, output_path, input_format, output_format):
    """Convert file content based on the specified formats."""
    try:
        if input_format == "txt" and output_format == "csv":
            with open(input_path, "r") as f:
                content = f.read()
            with open(output_path, "w") as f:
                f.write(content.upper())
        elif input_format == "txt" and output_format == "pdf":
            with open(input_path, "r") as f:
                content = f.read()
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Arial", size=12)
            pdf.multi_cell(0, 10, content)
            pdf.output(output_path)
        elif input_format == "csv" and output_format == "xlsx":
            df = pd.read_csv(input_path)
            df.to_excel(output_path, index=False)
        elif input_format == "docx" and output_format == "pdf":
            doc = Document(input_path)
            pdf = FPDF()
            pdf.add_page()
            for para in doc.paragraphs:
                pdf.set_font("Arial", size=12)
                pdf.multi_cell(0, 10, para.text)
            pdf.output(output_path)
        else:
            raise ValueError("Unsupported conversion format")
        
        logging.info(f"✅ Converted: '{input_path}' -> '{output_path}'")
        log_to_mongodb(task_name, input_path, output_path, "Converted")
    except Exception as e:
        logging.error(f"❌ Error converting '{input_path}': {e}")
        log_to_mongodb(task_name, input_path, output_path, f"Error: {e}", level="ERROR")

def compress_files(task_name, directory, output_path, compression_format):
    """Compress files in the specified directory."""
    try:
        if compression_format == "zip":
            with zipfile.ZipFile(output_path, 'w') as zipf:
                for root, _, files in os.walk(directory):
                    for file in files:
                        zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), directory))
        elif compression_format == "tar":
            with tarfile.open(output_path, 'w') as tarf:
                tarf.add(directory, arcname=os.path.basename(directory))
        else:
            raise ValueError("Unsupported compression format")
        
        logging.info(f"✅ Compressed: '{directory}' -> '{output_path}'")
        log_to_mongodb(task_name, directory, output_path, "Compressed")
    except Exception as e:
        logging.error(f"❌ Error compressing '{directory}': {e}")
        log_to_mongodb(task_name, directory, output_path, f"Error: {e}", level="ERROR")

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
            convert_file(task_name, input_path, output_path, input_format, output_format)
            files_converted += 1
    
    if files_converted == 0:
        log_to_mongodb(task_name, directory, None, f"No files with .{input_format} format found")

def file_compression_task(task_name, directory, compression_format):
    """Perform file compression for the specified directory."""
    if not os.path.exists(directory) or not os.path.isdir(directory):
        logging.error(f"Task '{task_name}' failed: Directory '{directory}' not found.")
        log_to_mongodb(task_name, directory, None, "Directory not found", level="ERROR")
        return

    output_filename = f"{os.path.basename(directory)}_{int(time.time())}.{compression_format}"
    output_path = os.path.join(directory, output_filename)
    compress_files(task_name, directory, output_path, compression_format)

def add_task(interval, unit, directory, task_type, input_format=None, output_format=None, compression_format=None):
    """Schedule a new file task (conversion or compression)."""
    scheduled_tasks = load_tasks()
    task_name = f"task_{interval}_{unit}_{task_type}"

    if task_type == "conversion":
        task_name += f"_{input_format}_{output_format}"
    elif task_type == "compression":
        task_name += f"_{compression_format}"

    if task_name in scheduled_tasks:
        print(f"⚠️ Task '{task_name}' is already scheduled.")
        return

    if unit == "seconds":
        trigger = IntervalTrigger(seconds=interval)
    elif unit == "minutes":
        trigger = IntervalTrigger(minutes=interval)
    elif unit == "hours":
        trigger = IntervalTrigger(hours=interval)
    elif unit == "days":
        trigger = IntervalTrigger(days=interval)
    else:
        print(f"⚠️ Unsupported time unit '{unit}'. Task not scheduled.")
        return

    if task_type == "conversion":
        if not input_format or not output_format:
            print("⚠️ Please provide --input-format and --output-format for conversion tasks.")
            return
        scheduler.add_job(
            file_conversion_task,
            trigger,
            args=[task_name, directory, input_format, output_format],
            id=task_name,
            replace_existing=True
        )
    elif task_type == "compression":
        if not compression_format:
            print("⚠️ Please provide --format for compression tasks.")
            return
        scheduler.add_job(
            file_compression_task,
            trigger,
            args=[task_name, directory, compression_format],
            id=task_name,
            replace_existing=True
        )
    else:
        print(f"⚠️ Unsupported task type '{task_type}'. Task not scheduled.")
        return
    
    scheduled_tasks[task_name] = {
        "interval": interval,
        "unit": unit,
        "directory": directory,
        "task_type": task_type,
        "input_format": input_format,
        "output_format": output_format,
        "compression_format": compression_format
    }
    save_tasks(scheduled_tasks)
    log_to_mongodb(task_name, directory, None, f"Task scheduled every {interval} {unit}")
    print(f"✅ Task '{task_name}' scheduled every {interval} {unit}.")

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
            task_type = details.get("task_type", "N/A")
            if task_type == "conversion":
                print(f" - {task_name}: Every {details['interval']} {details['unit']} | Dir: {directory} | Input: .{details['input_format']} | Output: .{details['output_format']}")
            elif task_type == "compression":
                print(f" - {task_name}: Every {details['interval']} {details['unit']} | Dir: {directory} | Format: {details['compression_format']}")

def load_and_schedule_tasks():
    """Load tasks from storage and schedule them."""
    scheduled_tasks = load_tasks()
    for task_name, details in scheduled_tasks.items():
        if not all(k in details for k in ["interval", "unit", "directory", "task_type"]):
            print(f"⚠️ Skipping task '{task_name}': Missing required fields.")
            continue

        if details["unit"] == "seconds":
            trigger = IntervalTrigger(seconds=details["interval"])
        elif details["unit"] == "minutes":
            trigger = IntervalTrigger(minutes=details["interval"])
        elif details["unit"] == "hours":
            trigger = IntervalTrigger(hours=details["interval"])
        elif details["unit"] == "days":
            trigger = IntervalTrigger(days=details["interval"])
        else:
            print(f"⚠️ Unsupported time unit '{details['unit']}'. Task not scheduled.")
            continue

        if details["task_type"] == "conversion":
            scheduler.add_job(
                file_conversion_task,
                trigger,
                args=[
                    task_name,
                    details.get("directory", "N/A"),
                    details.get("input_format", "txt"),
                    details.get("output_format", "csv")
                ],
                id=task_name,
                replace_existing=True
            )
        elif details["task_type"] == "compression":
            scheduler.add_job(
                file_compression_task,
                trigger,
                args=[
                    task_name,
                    details.get("directory", "N/A"),
                    details.get("compression_format", "zip")
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
parser = argparse.ArgumentParser(description="Background File Task Scheduler")
parser.add_argument("--add", type=int, help="Add a new task with interval")
parser.add_argument("--unit", type=str, choices=["seconds", "minutes", "hours", "days"], help="Time unit for the interval")
parser.add_argument("--dir", type=str, help="Directory to scan for files")
parser.add_argument("--task-type", type=str, choices=["conversion", "compression"], help="Type of task (conversion or compression)")
parser.add_argument("--input-format", type=str, help="Input file format (e.g., txt, csv, docx) for conversion tasks")
parser.add_argument("--output-format", type=str, help="Output file format (e.g., csv, xlsx, pdf) for conversion tasks")
parser.add_argument("--format", type=str, choices=["zip", "tar"], help="Compression format (e.g., zip, tar) for compression tasks")
parser.add_argument("--list", action="store_true", help="List all scheduled tasks")
parser.add_argument("--remove", type=str, help="Remove a scheduled task by name")

args = parser.parse_args()

# Load and schedule tasks before parsing commands
load_and_schedule_tasks()

if args.add:
    if not args.unit or not args.dir or not args.task_type:
        print("⚠️ Please provide --unit, --dir, and --task-type.")
        exit(1)
    if args.task_type == "conversion":
        if not args.input_format or not args.output_format:
            print("⚠️ Please provide --input-format and --output-format for conversion tasks.")
            exit(1)
        add_task(args.add, args.unit, args.dir, args.task_type, args.input_format, args.output_format)
    elif args.task_type == "compression":
        if not args.format:
            print("⚠️ Please provide --format for compression tasks.")
            exit(1)
        add_task(args.add, args.unit, args.dir, args.task_type, compression_format=args.format)
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

