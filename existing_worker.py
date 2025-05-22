# existing_worker.py
import os
import sys
import time
import argparse
from pathlib import Path

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--coordinator", required=True)
parser.add_argument("--name", required=True)
parser.add_argument("--api-url", required=True)
parser.add_argument("--model", required=True)
parser.add_argument("--worker-id", required=True)
parser.add_argument("--api-key")
args = parser.parse_args()

# Import the DocumentWorker class from worker.py
sys.path.insert(0, str(Path(__file__).parent))
from worker import DocumentWorker

# Create worker instance
worker = DocumentWorker(
    args.coordinator,
    args.name,
    args.api_url,
    args.model,
    args.api_key
)

# Skip registration by directly setting the worker ID
worker.worker_id = args.worker_id
print(f"Using existing worker ID: {worker.worker_id}")

# Override register method to prevent new registration
worker.register = lambda: True

# Set up initial state and heartbeat
worker.last_heartbeat = time.time()
worker.current_state = "idle"

# Run the worker (this will start processing without registration)
print(f"Starting worker with existing ID: {worker.worker_id}")
worker.run()