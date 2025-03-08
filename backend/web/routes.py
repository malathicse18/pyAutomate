from backend.web import app
from flask import jsonify, request
from backend.job_scheduler.scheduler import add_task, remove_task, list_tasks
from backend.database.db_utils import get_logs

@app.route('/tasks', methods=['POST'])
def schedule_task():
    """Endpoint to schedule a new task."""
    data = request.get_json()
    if not data or not all(k in data for k in ["interval", "unit", "directory", "task_type"]):
        return jsonify({"message": "Missing required fields: interval, unit, directory, task_type"}), 400

    interval = data["interval"]
    unit = data["unit"]
    directory = data["directory"]
    task_type = data["task_type"]
    compression_format = data.get("compression_format")

    return add_task(interval, unit, directory, task_type, compression_format)

@app.route('/tasks/<task_name>', methods=['DELETE'])
def unschedule_task(task_name):
    """Endpoint to remove a task."""
    return remove_task(task_name)

@app.route('/tasks', methods=['GET'])
def get_tasks():
    """Endpoint to list all tasks."""
    return list_tasks()

@app.route('/logs', methods=['GET'])
def get_logs_route():
    """Endpoint to fetch logs from MongoDB."""
    return jsonify(get_logs())