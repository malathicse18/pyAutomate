import argparse
import json
import logging
import time
import os
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Configure logging
logging.basicConfig(filename="file_conversion.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Persistent Task Storage File
TASK_FILE = "scheduled_tasks.json"

# Initialize Scheduler (BlockingScheduler ensures script doesn't exit early)
scheduler = BlockingScheduler()


def load_tasks():
    """Load scheduled tasks from file."""
    try:
        with open(TASK_FILE, "r") as f:
            tasks = json.load(f)
            return tasks if isinstance(tasks, dict) else {}  # Ensure dictionary
    except (FileNotFoundError, json.JSONDecodeError):
        return {}  # Return empty if file is missing/corrupt


def save_tasks(tasks):
    """Save scheduled tasks to file."""
    with open(TASK_FILE, "w") as f:
        json.dump(tasks, f, indent=4)


def file_conversion_task(task_name, input_file, output_file):
    """Perform file conversion task."""
    abs_input_path = os.path.abspath(input_file)
    abs_output_path = os.path.abspath(output_file)

    print(f"🔍 Checking paths...\n📂 Input File: {abs_input_path}\n📂 Output File: {abs_output_path}")

    if not os.path.exists(input_file):
        logging.error(f"Task '{task_name}' failed: Input file '{input_file}' not found.")
        print(f"❌ Task '{task_name}' failed: Input file '{input_file}' not found.")
        return

    try:
        with open(input_file, "r") as f:
            content = f.read()

        with open(output_file, "w") as f:
            f.write(content.upper())

        logging.info(f"✅ Task executed: {task_name} (Converted '{input_file}' -> '{output_file}')")
        print(f"✅ Task executed: {task_name} (Converted '{input_file}' -> '{output_file}')")
    except Exception as e:
        logging.error(f"❌ Error in task '{task_name}': {e}")
        print(f"❌ Error in task '{task_name}': {e}")



def add_task(task_name, interval, input_file, output_file):
    """Schedule a new file conversion task."""
    scheduled_tasks = load_tasks()

    if task_name in scheduled_tasks:
        print(f"⚠️ Task '{task_name}' is already scheduled.")
        return

    scheduler.add_job(
        file_conversion_task,
        IntervalTrigger(seconds=interval),
        args=[task_name, input_file, output_file],
        id=task_name
    )

    scheduled_tasks[task_name] = {
        "interval": interval,
        "input_file": input_file,
        "output_file": output_file
    }
    save_tasks(scheduled_tasks)
    print(f"✅ Task '{task_name}' scheduled every {interval} seconds.")


def remove_task(task_name):
    """Remove a scheduled task."""
    scheduled_tasks = load_tasks()

    if task_name in scheduled_tasks:
        scheduler.remove_job(task_name)
        del scheduled_tasks[task_name]
        save_tasks(scheduled_tasks)
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
            print(f" - {task_name}: Runs every {details['interval']}s | Input: {details['input_file']} | Output: {details['output_file']}")


# Load existing tasks when starting the script
scheduled_tasks = load_tasks()
for task_name, details in scheduled_tasks.items():
    scheduler.add_job(
        file_conversion_task,
        IntervalTrigger(seconds=details["interval"]),
        args=[task_name, details["input_file"], details["output_file"]],
        id=task_name
    )

# Print Active Jobs Before Scheduler Starts
print("📌 Active Scheduled Jobs Before Starting:")
for job in scheduler.get_jobs():
    print(job)

# CLI Argument Parsing
parser = argparse.ArgumentParser(description="Background File Conversion Scheduler")
parser.add_argument("--add", type=int, help="Add a new task with interval (in seconds)")
parser.add_argument("--input", type=str, help="Input file for conversion")
parser.add_argument("--output", type=str, help="Output file after conversion")
parser.add_argument("--list", action="store_true", help="List all scheduled tasks")
parser.add_argument("--remove", type=str, help="Remove a scheduled task by name")

args = parser.parse_args()

if args.add:
    if not args.input or not args.output:
        print("⚠️ Please provide both --input and --output files.")
        exit(1)
    add_task(f"task_{args.add}", args.add, args.input, args.output)
    exit(0)

elif args.list:
    list_tasks()
    exit(0)

elif args.remove:
    remove_task(args.remove)
    exit(0)

else:
    print("⚠️ Invalid arguments. Use --help for usage details.")

# Keep the script running
try:
    print("🚀 Scheduler is running... Press Ctrl+C to stop.")
    scheduler.start()
except KeyboardInterrupt:
    print("🛑 Scheduler stopped.")
    scheduler.shutdown()
