# worker.py
import os
import time
import json
import requests
import uuid
import argparse
import sys
from pathlib import Path

from parser_utils import run_parser
import redis
from redis import ConnectionPool

# Initialize connection pool at module level for reuse
REDIS_POOL = ConnectionPool(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True,
    max_connections=10,  # Adjust based on your needs
    socket_timeout=5,
    socket_connect_timeout=2
)

class WorkerState:
    IDLE = "idle"
    PROCESSING = "processing"
    STOPPED = "stopped"
    ERROR = "error"
    REMOVING = "removing"


class DocumentWorker:
    def __init__(self, coordinator_url, worker_name, api_url, model, api_key=None):
        self.coordinator_url = coordinator_url
        self.worker_name = worker_name
        self.api_url = api_url
        self.model = model
        self.worker_id = None
        self.running = True
        self.heartbeat_interval = 10  # seconds
        self.last_heartbeat = 0
        self.current_state = WorkerState.IDLE

        # Use the connection pool instead of creating a new connection
        self.redis_client = redis.Redis(connection_pool=REDIS_POOL)
        # API key handling
        if not api_key and "openai.com" in api_url:
            print("WARNING: OpenAI API endpoint specified without API key")
        self.api_key = api_key

        # Add direct Redis update method

    def update_status_in_redis(self, status):
        """Directly update worker status in Redis."""
        if not self.worker_id:
            return False

        try:
            self.redis_client.hset(f"worker:{self.worker_id}", "status", status)
            return True
        except Exception as e:
            print(f"Error updating Redis status: {e}")
            return False

    def register(self):
        """Register worker with the coordinator."""
        registration_data = {
            "worker_name": self.worker_name,
            "api_url": self.api_url,
            "model": self.model,
            "api_key": self.api_key,
            "process_id": str(os.getpid())  # Convert to string
        }

        try:
            response = requests.post(
                f"{self.coordinator_url}/api/register-worker",
                json=registration_data
            )

            if response.status_code != 200:
                print(f"Failed to register worker: {response.text}")
                return False

            data = response.json()
            self.worker_id = data["worker_id"]
            print(f"Worker registered with ID: {self.worker_id}")

            if "warning" in data:
                print(data["warning"])

            return True
        except Exception as e:
            print(f"Error registering worker: {e}")
            return False

    def send_heartbeat(self, status=None, document_id=None):
        """Send heartbeat to coordinator."""
        if status:
            self.current_state = status

        if time.time() - self.last_heartbeat < self.heartbeat_interval:
            return

        heartbeat_data = {
            "worker_id": self.worker_id,
            "status": status or self.current_state,
            "document_id": document_id
        }

        try:
            response = requests.post(
                f"{self.coordinator_url}/api/worker-heartbeat",
                json=heartbeat_data
            )

            if response.status_code == 200:
                result = response.json()
                if result.get("command") == "shutdown" or result.get("command") == "remove":
                    print("Received shutdown/remove command from coordinator")
                    self.running = False
                    sys.exit(0)  # Exit the process immediately
                elif result.get("command") == "stop":
                    print("Received stop command from coordinator")
                    self.current_state = WorkerState.STOPPED

            self.last_heartbeat = time.time()
        except Exception as e:
            print(f"Error sending heartbeat: {e}")
            self.current_state = WorkerState.ERROR

    def get_next_document(self):
        """Get next document from queue."""
        if self.current_state in [WorkerState.STOPPED, WorkerState.ERROR, WorkerState.REMOVING]:
            return None

        try:
            response = requests.get(
                f"{self.coordinator_url}/api/next-document/{self.worker_id}"
            )

            if response.status_code != 200:
                return None

            result = response.json()
            if result.get("status") == "No documents in queue" or result.get(
                    "status") == "Worker is not in active state":
                return None

            return result.get("document")
        except Exception as e:
            print(f"Error getting next document: {e}")
            self.current_state = WorkerState.ERROR
            self.send_error(str(e))
            return None

    def send_error(self, error_message, document_id=None):
        """Report error to coordinator."""
        error_data = {
            "worker_id": self.worker_id,
            "document_id": document_id,
            "error": error_message
        }

        try:
            requests.post(
                f"{self.coordinator_url}/api/worker-error",
                json=error_data
            )
        except Exception as e:
            print(f"Error reporting worker error: {e}")

    # Modify the process_document method in worker.py
    def process_document(self, document):
        """Process a document with the configured LLM."""
        document_id = document["id"]
        file_path = document["path"]
        schema_name = document.get("schema_name", "*")

        self.send_heartbeat(WorkerState.PROCESSING, document_id)

        try:
            # Process document using existing parser
            result = run_parser(
                file_path,
                self.api_url,
                model=self.model,
                api_key=self.api_key,
                query="*",
                type="schema",
                schema=schema_name
            )

            is_error = False
            if isinstance(result, dict) and ("error" in result or "Error" in result or result.get("success") is False):
                is_error = True

            # Send result to coordinator for MongoDB storage
            response = requests.post(
                f"{self.coordinator_url}/api/document-processed",
                params={
                    "worker_id": self.worker_id,
                    "document_id": document_id
                },
                json={
                    "is_error": is_error,
                    "file_path": file_path,
                    "schema_name": schema_name,
                    "result": result
                }
            )

            if response.status_code != 200:
                print(f"Warning: Error storing result in MongoDB: {response.text}")

            print(f"Document processed: {Path(file_path).name}")
            self.send_heartbeat(WorkerState.IDLE)
            return True
        except Exception as e:
            error_message = f"Error processing document {document_id}: {e}"
            print(error_message)
            self.current_state = WorkerState.ERROR
            self.send_error(error_message, document_id)
            return False

    def run(self):
        """Main worker loop."""
        if not self.register():
            return

        print(f"Worker started with model: {self.model}")

        try:
            while self.running:
                try:
                    # Send heartbeat
                    self.send_heartbeat()

                    # Check if stopped
                    if self.current_state in [WorkerState.STOPPED, WorkerState.REMOVING]:
                        time.sleep(1)
                        continue

                    # Get next document
                    document = self.get_next_document()
                    if not document:
                        time.sleep(1)
                        continue

                    # Process document
                    self.process_document(document)
                except Exception as e:
                    print(f"Error in worker loop: {e}")
                    self.current_state = WorkerState.ERROR
                    self.send_error(str(e))
                    time.sleep(5)
        except KeyboardInterrupt:
            print("Worker stopping...")
            self.current_state = WorkerState.STOPPED

            # Direct update in Redis
            if self.update_status_in_redis(WorkerState.STOPPED):
                print("Worker status updated to STOPPED in Redis")
                time.sleep(0.5)
            # Still try the heartbeat as backup
            self.send_heartbeat(status=WorkerState.STOPPED)
        finally:
            # Ensure proper shutdown and status update
            if self.current_state != WorkerState.STOPPED:
                self.update_status_in_redis(WorkerState.STOPPED)
            print("Worker stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Document Processing Worker")
    parser.add_argument("--coordinator", default="http://localhost:8000", help="Coordinator URL")
    parser.add_argument("--name", default=f"worker-{uuid.uuid4().hex[:8]}", help="Worker name")
    parser.add_argument("--api-url", default="https://api.openai.com/v1/chat/completions", help="LLM API URL")
    parser.add_argument("--model", default="gpt-4o-mini", help="LLM model name")
    parser.add_argument("--api-key", default=os.getenv("OPENAI_API_KEY"), help="LLM API key")


    args = parser.parse_args()

    # API key is optional now
    worker = DocumentWorker(
        args.coordinator,
        args.name,
        args.api_url,
        args.model,
        args.api_key
    )

    worker.run()