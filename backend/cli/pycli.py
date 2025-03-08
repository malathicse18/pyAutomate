import argparse
import requests
import json
import sys

def main():
    parser = argparse.ArgumentParser(description="Automate CLI")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Schedule task command (create parser first)
    schedule_parser = subparsers.add_parser('schedule', help='Schedule a new task')
    schedule_parser.add_argument('interval', type=int, help='Task interval')
    schedule_parser.add_argument('unit', choices=['seconds', 'minutes', 'hours', 'days'], help='Interval unit')
    schedule_parser.add_argument('task_type', choices=['compression', 'other'], help='Task type')
    schedule_parser.add_argument('directory', help='Directory to operate on')
    schedule_parser.add_argument('--compression_format', choices=['zip', 'tar'], help='Compression format (for compression tasks)')

    # Remove task command (create parser first)
    remove_parser = subparsers.add_parser('remove', help='Remove a scheduled task')
    remove_parser.add_argument('task_name', help='Name of the task to remove')

    # List tasks command
    subparsers.add_parser('list', help='List all scheduled tasks')

    # List logs command
    subparsers.add_parser('logs', help='List application logs')

    args = parser.parse_args()

    base_url = "http://127.0.0.1:5000"  # Adjust if your Flask app runs on a different address/port

    if args.command == 'schedule':
        payload = {
            "interval": args.interval,
            "unit": args.unit,
            "directory": args.directory,
            "task_type": args.task_type,
        }
        if args.compression_format:
            payload["compression_format"] = args.compression_format

        try:
            response = requests.post(f"{base_url}/tasks", json=payload)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            print(response.json()['message'])
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            sys.exit(1)

    elif args.command == 'remove':
        try:
            response = requests.delete(f"{base_url}/tasks/{args.task_name}")
            response.raise_for_status()
            print(response.json()['message'])
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            sys.exit(1)

    elif args.command == 'list':
        try:
            response = requests.get(f"{base_url}/tasks")
            response.raise_for_status()
            tasks = response.json().get("tasks", [])
            if tasks:
                print("Scheduled Tasks:")
                for task in tasks:
                    print(task)
            else:
                print(response.json().get("message", "No tasks found."))
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            sys.exit(1)

    elif args.command == 'logs':
        try:
            response = requests.get(f"{base_url}/logs")
            response.raise_for_status()
            logs = response.json()
            if logs:
                print("Application Logs:")
                for log in logs:
                    print(f"[{log['timestamp']}] {log['level']}: {log['status']} - Task: {log.get('task_name', 'N/A')}, Dir: {log.get('directory', 'N/A')}, Output: {log.get('output', 'N/A')}")
            else:
                print("No logs found.")
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            sys.exit(1)

    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()