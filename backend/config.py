import os
from dotenv import load_dotenv
from pathlib import Path  # Import Path from pathlib

# Construct the path to the .env file in the root directory
env_path = Path(__file__).resolve().parents[1] / '.env'

load_dotenv(dotenv_path=env_path)

MONGO_URI = os.getenv("MONGO_URI")
FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY")