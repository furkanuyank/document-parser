# coordinator.py
import os
import time
import redis
from redis import ConnectionPool
import json
import uuid
from fastapi import FastAPI, File, UploadFile, Form, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel
import uvicorn
from pathlib import Path
from enum import Enum
from pymongo import MongoClient

# Add MongoDB connection setup after Redis setup
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
mongo_client = MongoClient(MONGO_URI)
documents_db = mongo_client["document_processing"]
results_collection = documents_db["processing_results"]
errors_collection = documents_db["processing_errors"]

app = FastAPI(title="Document Processing Coordinator")

# Initialize connection pool
REDIS_POOL = ConnectionPool(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True,
    max_connections=20,  # Higher for coordinator as it handles more connections
    socket_timeout=5,
    socket_connect_timeout=2
)

# Get a client from the pool when needed
redis_client = redis.Redis(connection_pool=REDIS_POOL)

# Constants remain the same
DOCUMENT_QUEUE = "document_queue"
PROCESSING_SET = "processing_documents"
PROCESSED_COUNTER = "processed_documents_count"
ERROR_COUNTER = "error_documents_count"
WORKERS_SET = "active_workers"
WORKER_HEARTBEAT_TIMEOUT = 30
SCHEMAS_SET = "available_schemas"

# Ensure results folder exists
# os.makedirs(RESULTS_FOLDER, exist_ok=True)


# Worker states
class WorkerState(str, Enum):
    IDLE = "idle"
    PROCESSING = "processing"
    STOPPED = "stopped"
    ERROR = "error"
    REMOVING = "removing"


# Models
class WorkerRegistration(BaseModel):
    worker_name: str
    api_url: str
    model: str
    api_key: str = None
    process_id: str = None  # Add this field

class WorkerStatus(BaseModel):
    worker_id: str
    document_id: str = None
    status: str


# API Endpoints

@app.get("/")
async def root():
    return {"status": "Document Processing System Online"}

@app.get("/api/debug/routes")
async def debug_routes():
    """List all registered routes for debugging."""
    routes = []
    for route in app.routes:
        routes.append({
            "path": route.path,
            "name": route.name,
            "methods": route.methods if hasattr(route, "methods") else None
        })
    return {"routes": routes}

@app.post("/api/enqueue")
async def enqueue_document(file_path: str, schema_name: str = None):
    """Add a document path to the processing queue."""
    document_id = str(uuid.uuid4())
    document_data = {
        "id": document_id,
        "path": file_path,
        "status": "queued",
        "enqueued_at": time.time()
    }

    # Add schema if provided
    if schema_name:
        document_data["schema_name"] = schema_name

    # Add to queue
    redis_client.lpush(DOCUMENT_QUEUE, json.dumps(document_data))

    return {
        "status": "Document enqueued",
        "document_id": document_id,
        "queue_position": redis_client.llen(DOCUMENT_QUEUE),
        "schema": schema_name if schema_name else "default"
    }

@app.post("/api/enqueue-folder")
async def enqueue_folder(folder_path: str, schema_name: str = None):
    """Add all documents in a folder to the processing queue."""
    path = Path(folder_path)
    enqueued = 0

    if not path.exists() or not path.is_dir():
        return {"error": f"Folder not found or not a directory: {folder_path}"}

    # valid_extensions = ['.pdf', '.png', '.jpg', '.jpeg', '.tiff', '.bmp','.txt']

    for file_path in path.glob('**/*'):
        if file_path.is_file():
            document_id = str(uuid.uuid4())
            document_data = {
                "id": document_id,
                "path": str(file_path),
                "status": "pending",
                "created_at": time.time()
            }

            # Add schema if provided
            if schema_name:
                document_data["schema_name"] = schema_name

            # Add to queue
            redis_client.lpush(DOCUMENT_QUEUE, json.dumps(document_data))
            enqueued += 1

    return {
        "status": "Folder documents enqueued",
        "count": enqueued,
        "folder": folder_path,
        "schema": schema_name if schema_name else "default"
    }

@app.post("/api/register-worker")
async def register_worker(worker: WorkerRegistration):
    """Register a new worker with the system."""
    worker_id = str(uuid.uuid4())

    # Check API key if using OpenAI endpoint
    api_key_warning = None
    if "openai.com" in worker.api_url and not worker.api_key:
        api_key_warning = "WARNING: OpenAI API endpoint specified without API key"

    worker_data = {
        "id": worker_id,
        "name": worker.worker_name,
        "api_url": worker.api_url,
        "model": worker.model,
        "api_key": worker.api_key or "",
        "status": WorkerState.IDLE,
        "registered_at": time.time(),
        "last_heartbeat": time.time(),
        "processed_documents": 0,
        "errors": 0
    }

    # Register worker
    redis_client.hset(f"worker:{worker_id}", mapping=worker_data)
    redis_client.sadd(WORKERS_SET, worker_id)

    response = {
        "status": "Worker registered",
        "worker_id": worker_id,
        "config": {
            "api_url": worker.api_url,
            "model": worker.model
        }
    }

    if api_key_warning:
        response["warning"] = api_key_warning

    return response

@app.delete("/api/force-remove-worker/{worker_id}")
async def force_remove_worker(worker_id: str):
    """Forcefully remove a worker from the system."""
    if not redis_client.sismember(WORKERS_SET, worker_id):
        return {"error": "Worker not found"}

    # Remove from active workers set
    redis_client.srem(WORKERS_SET, worker_id)

    # Delete worker data
    redis_client.delete(f"worker:{worker_id}")

    return {"status": "Worker forcefully removed", "worker_id": worker_id}

@app.post("/api/worker-heartbeat")
async def worker_heartbeat(request: Request):
    """Update worker heartbeat and status."""
    try:
        status_data = await request.json()
        worker_id = status_data.get("worker_id")
        status = status_data.get("status")
        document_id = status_data.get("document_id")

        if not worker_id:
            return {"error": "Worker ID is required"}

        if not redis_client.sismember(WORKERS_SET, worker_id):
            return {"error": "Worker not registered"}

        # Get current worker state
        current_state = redis_client.hget(f"worker:{worker_id}", "status")

        # Handle state transitions
        if current_state == WorkerState.REMOVING:
            return {"command": "shutdown"}
        elif current_state == WorkerState.STOPPED and status != WorkerState.ERROR:
            # If worker is stopped, don't update status unless it's an error report
            # Just update heartbeat
            redis_client.hset(
                f"worker:{worker_id}",
                mapping={
                    "last_heartbeat": time.time()
                }
            )
            return {"command": "stop"}
        else:
            # Update heartbeat and status
            redis_client.hset(
                f"worker:{worker_id}",
                mapping={
                    "last_heartbeat": time.time(),
                    "status": status,
                    "current_document": document_id or ""
                }
            )

        # Return appropriate command based on worker state
        if current_state == WorkerState.REMOVING:
            return {"command": "shutdown"}
        elif current_state == WorkerState.STOPPED:
            return {"command": "stop"}

        return {"status": "Heartbeat received"}
    except Exception as e:
        print(f"Error processing heartbeat: {e}")
        return {"error": str(e)}

@app.post("/api/worker/stop/{worker_id}")
async def stop_worker(worker_id: str):
    """Stop a worker from processing documents."""
    if not redis_client.sismember(WORKERS_SET, worker_id):
        return {"error": "Worker not found"}

    # Set worker to STOPPED state
    redis_client.hset(f"worker:{worker_id}", "status", WorkerState.STOPPED)

    return {"status": "Worker stopped", "worker_id": worker_id}

@app.post("/api/worker/start/{worker_id}")
async def start_worker(worker_id: str):
    """Start a stopped worker."""
    if not redis_client.sismember(WORKERS_SET, worker_id):
        return {"error": "Worker not found"}

    current_status = redis_client.hget(f"worker:{worker_id}", "status")
    if current_status not in [WorkerState.STOPPED, WorkerState.ERROR]:
        return {"error": f"Worker cannot be started from {current_status} state"}

    # Set worker to IDLE state
    redis_client.hset(f"worker:{worker_id}", "status", WorkerState.IDLE)

    return {"status": "Worker started", "worker_id": worker_id}

@app.get("/api/next-document/{worker_id}")
async def get_next_document(worker_id: str):
    """Get the next document for a worker to process."""
    if not redis_client.sismember(WORKERS_SET, worker_id):
        return {"error": "Worker not registered"}

    # Always update heartbeat when worker requests a document
    redis_client.hset(f"worker:{worker_id}", "last_heartbeat", time.time())

    # Check worker state
    worker_status = redis_client.hget(f"worker:{worker_id}", "status")
    if worker_status in [WorkerState.STOPPED, WorkerState.ERROR, WorkerState.REMOVING]:
        return {"status": "Worker is not in active state", "worker_state": worker_status}

    # Use Redis atomic operation to move item from queue to processing
    document_data_str = redis_client.brpoplpush(DOCUMENT_QUEUE, PROCESSING_SET, timeout=1)

    if not document_data_str:
        return {"status": "No documents in queue"}

    document_data = json.loads(document_data_str)
    document_id = document_data["id"]

    # Update worker state to PROCESSING
    redis_client.hset(f"worker:{worker_id}", "status", WorkerState.PROCESSING)

    # Assign document to worker
    redis_client.hset(
        f"document:{document_id}",
        mapping={
            "worker_id": worker_id,
            "processing_started": time.time()
        }
    )

    return {
        "status": "Document assigned",
        "document": document_data
    }


@app.post("/api/document-processed")
async def document_processed(request: Request):
    # Get parameters from query params
    worker_id = request.query_params.get("worker_id")
    document_id = request.query_params.get("document_id")
    result_data = await request.json()

    if not worker_id or not document_id:
        return {"error": "Missing required parameters: worker_id and document_id"}

    if not redis_client.sismember(WORKERS_SET, worker_id):
        return {"error": "Worker not registered"}

    # Get the result data from the request body
    try:
        result_data = await request.json()
        is_error = result_data.get("is_error", False)

        # Define mongo_document before the conditional branches
        mongo_document = {
            "worker_id": worker_id,
            "file_path": result_data.get("file_path"),
            "schema_name": result_data.get("schema_name"),
            "result": result_data.get("result"),
            "processed_at": time.time()
        }

        if is_error:
            # Store in errors collection
            insert_result = errors_collection.insert_one(mongo_document)
            redis_client.hincrby(f"worker:{worker_id}", "errors", 1)
            redis_client.incr(ERROR_COUNTER)
        else:
            # Store the result in MongoDB
            insert_result = results_collection.insert_one(mongo_document)
    except Exception as e:
        print(f"Error storing result in MongoDB: {e}")

    # Rest of the existing code for Redis operations...
    # Remove from processing set
    try:
        processing_items = redis_client.lrange(PROCESSING_SET, 0, -1)
        for i, item in enumerate(processing_items):
            try:
                item_data = json.loads(item)
                if item_data.get("id") == document_id:
                    redis_client.lrem(PROCESSING_SET, 1, item)
                    break
            except:
                continue
    except Exception as e:
        print(f"Error removing from processing set: {e}")

    # Update worker status
    redis_client.hset(
        f"worker:{worker_id}",
        mapping={
            "status": WorkerState.IDLE,
            "current_document": ""
        }
    )

    # Increment processed documents counter for system
    redis_client.incr(PROCESSED_COUNTER)

    # Increment processed documents counter for worker
    redis_client.hincrby(f"worker:{worker_id}", "processed_documents", 1)

    # Delete document from Redis after processing is complete
    redis_client.delete(f"document:{document_id}")

    return {"status": "Document processed and result saved to MongoDB"}

@app.get("/api/worker/{worker_id}")
async def get_worker_status(worker_id: str):
    """Get detailed worker status."""
    if not redis_client.sismember(WORKERS_SET, worker_id):
        return {"error": "Worker not found"}

    worker_data = redis_client.hgetall(f"worker:{worker_id}")

    # Remove the unresponsive check
    # No longer checking for heartbeat timeout

    # Get statistics
    stats = {
        "processed_documents": int(worker_data.get("processed_documents", 0)),
        "errors": int(worker_data.get("errors", 0)),
        "uptime": time.time() - float(worker_data.get("registered_at", time.time()))
    }

    return {
        "worker": worker_data,
        "stats": stats
    }

@app.get("/api/system-status")
async def get_system_status():
    """Get current system status."""
    # Get document queue stats
    pending_count = redis_client.llen(DOCUMENT_QUEUE)
    processing_count = redis_client.llen(PROCESSING_SET)
    processed_count = int(redis_client.get(PROCESSED_COUNTER) or 0)  # Get processed count
    error_count = int(redis_client.get(ERROR_COUNTER) or 0)  # Get error count
    # Get worker status
    workers = []
    for worker_id in redis_client.smembers(WORKERS_SET):
        worker_data = redis_client.hgetall(f"worker:{worker_id}")
        if worker_data:
            workers.append({
                "id": worker_id,
                "name": worker_data.get("name", "Unknown"),
                "status": worker_data.get("status", "unknown"),
                "model": worker_data.get("model", "unknown"),
                # "processed_documents": int(worker_data.get("processed_documents", 0)),
                # "last_heartbeat": float(worker_data.get("last_heartbeat", 0))
            })

    return {
        "queue_status": {
            "pending": pending_count,
            "processing": processing_count,
            "processed": processed_count,
            "errors": error_count
        },
        "workers": workers
    }

@app.post("/api/schema")
async def add_schema(request: Request):
    """Add a schema to the system."""
    try:
        schema_data = await request.json()
        schema_name = schema_data.get("name")
        schema_content = schema_data.get("content")

        if not schema_name:
            return {"error": "Schema name is required"}

        # Store schema in Redis
        redis_client.hset(f"schema:{schema_name}", mapping={
            "name": schema_name,
            "content": json.dumps(schema_content),
            "created_at": time.time()
        })

        # Add to schemas set
        redis_client.sadd(SCHEMAS_SET, schema_name)

        return {
            "status": "Schema added successfully",
            "name": schema_name
        }
    except Exception as e:
        return {"error": f"Failed to add schema: {str(e)}"}

@app.get("/api/schemas")
async def get_schemas():
    """List all available schemas."""
    try:
        schema_names = redis_client.smembers(SCHEMAS_SET)
        schemas = []

        for name in schema_names:
            schema_data = redis_client.hgetall(f"schema:{name}")
            if schema_data:
                schemas.append({
                    "name": schema_data.get("name"),
                    "created_at": schema_data.get("created_at")
                })

        return {"schemas": schemas}
    except Exception as e:
        return {"error": f"Failed to retrieve schemas: {str(e)}"}

@app.get("/api/schema/{schema_name}")
async def get_schema(schema_name: str):
    """Get a schema by name."""
    try:
        # Check if schema exists
        if not redis_client.sismember(SCHEMAS_SET, schema_name):
            return {"error": "Schema not found"}

        # Get schema data
        schema_data = redis_client.hgetall(f"schema:{schema_name}")
        if not schema_data:
            return {"error": "Schema data not found"}

        # Parse content from string to JSON
        try:
            content = json.loads(schema_data.get("content", "{}"))
        except json.JSONDecodeError:
            content = {}

        return {
            "name": schema_name,
            "content": content,
            "created_at": schema_data.get("created_at")
        }
    except Exception as e:
        return {"error": f"Failed to retrieve schema: {str(e)}"}


@app.delete("/api/schema/{schema_name}")
async def delete_schema(schema_name: str):
    """Delete a schema by name."""
    try:
        # Check if schema exists
        if not redis_client.sismember(SCHEMAS_SET, schema_name):
            return {"error": "Schema not found"}

        # Delete schema data
        redis_client.delete(f"schema:{schema_name}")

        # Remove from schemas set
        redis_client.srem(SCHEMAS_SET, schema_name)

        return {
            "status": "Schema deleted successfully",
            "name": schema_name
        }
    except Exception as e:
        return {"error": f"Failed to delete schema: {str(e)}"}

if __name__ == "__main__":
    uvicorn.run("coordinator:app", host="localhost", port=8000, reload=True)