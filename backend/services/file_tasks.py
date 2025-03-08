import os
import zipfile
import tarfile
import logging
import time
from backend.database.db_utils import log_to_mongodb

log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "logs")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, "file_tasks.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def file_task(task_name, directory, compression_format=None):
    """Perform file compression or other tasks for the specified directory."""
    if not os.path.exists(directory) or not os.path.isdir(directory):
        logging.error(f"Task '{task_name}' failed: Directory '{directory}' not found.")
        log_to_mongodb(task_name, directory, None, "Directory not found", level="ERROR")
        return

    if compression_format:
        output_filename = f"{os.path.basename(directory)}_{int(time.time())}.{compression_format}"
        output_path = os.path.join(directory, output_filename)
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
    else:
        logging.info(f"Task '{task_name}' executed for directory '{directory}'.")
        log_to_mongodb(task_name, directory, None, "Task executed")