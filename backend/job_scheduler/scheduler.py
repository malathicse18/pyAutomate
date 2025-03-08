import json
import time
import threading
import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from backend.services.file_tasks import file_task
from backend.database.db_utils import log_to_mongodb
from flask import jsonify

scheduler = BackgroundScheduler()
TASK_FILE = "backend/job_scheduler/tasks.json"

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

def add_task(interval, unit, directory, task_type, compression_format=None):
    """Schedule a new file task (compression or other)."""
    scheduled_tasks = load_tasks()
    task_name = f"task_{interval}_{unit}_{task_type}"

    if task_type == "compression" and compression_format:
        task_name += f"_{compression_format}"

    if task_name in scheduled_tasks:
        return jsonify({"message": f"Task '{task_name}' is already scheduled."})

    if unit == "seconds":
        trigger = IntervalTrigger(seconds=interval)
    elif unit == "minutes":
        trigger = IntervalTrigger(minutes=interval)
    elif unit == "hours":
        trigger = IntervalTrigger(hours=interval)
    elif unit == "days":
        trigger = IntervalTrigger(days=interval)
    else:
        return jsonify({"message": f"Unsupported time unit '{unit}'. Task not scheduled."}), 400

    if task_type == "compression":
        if not compression_format:
            return jsonify({"message": "Please provide compression format."}), 400
        scheduler.add_job(
            file_task,
            trigger,
            args=[task_name, directory, compression_format],
            id=task_name,
            replace_existing=True
        )
    elif task_type == "other":
        scheduler.add_job(
            file_task,
            trigger,
            args=[task_name, directory],
            id=task_name,
            replace_existing=True
        )
    else:
        return jsonify({"message": f"Unsupported task type '{task_type}'. Task not scheduled."}), 400

    scheduled_tasks[task_name] = {
        "interval": interval,
        "unit": unit,
        "directory": directory,
        "task_type": task_type,
        "compression_format": compression_format
    }
    save_tasks(scheduled_tasks)
    log_to_mongodb(task_name, directory, None, f"Task scheduled every {interval} {unit}")
    return jsonify({"message": f"Task '{task_name}' scheduled every {interval} {unit}."}), 201

def remove_task(task_name):
    """Remove a scheduled task."""
    scheduled_tasks = load_tasks()
    if task_name in scheduled_tasks:
        scheduler.remove_job(task_name)
        del scheduled_tasks[task_name]
        save_tasks(scheduled_tasks)
        log_to_mongodb(task_name, None, None, "Task removed")
        return jsonify({"message": f"Task '{task_name}' removed."}), 200
    else:
        return jsonify({"message": f"No task found with name '{task_name}'."}), 404

def list_tasks():
    """List all active scheduled tasks."""
    scheduled_tasks = load_tasks()
    if not scheduled_tasks:
        return jsonify({"message": "No tasks scheduled.", "tasks": []}), 200
    else:
        task_list = []
        for task_name, details in scheduled_tasks.items():
            directory = details.get("directory", "N/A")
            task_type = details.get("task_type", "N/A")
            comp_format = details.get("compression_format", "N/A") if task_type == "compression" else "N/A"
            task_list.append(f" - {task_name}: Every {details['interval']} {details['unit']} | Dir: {directory} | Type: {task_type} | Format: {comp_format}")
        return jsonify({"message": "Scheduled tasks:", "tasks": task_list}), 200
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

        if details["task_type"] == "compression":
            scheduler.add_job(
                file_task,
                trigger,
                args=[
                    task_name,
                    details.get("directory", "N/A"),
                    details.get("compression_format", "zip")
                ],
                id=task_name,
                replace_existing=True
            )
        elif details["task_type"] == "other":
            scheduler.add_job(
                file_task,
                trigger,
                args=[
                    task_name,
                    details.get("directory", "N/A"),
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