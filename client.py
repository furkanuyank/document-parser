# client.py - Example client to submit documents and manage workers
import argparse
import requests
import os
import sys
import subprocess
from pathlib import Path
import json

def worker_name_exists(coordinator_url, worker_name):
    """Check if a worker with the given name already exists."""
    try:
        status = get_system_status(coordinator_url)
        for worker in status.get('workers', []):
            if worker.get('name') == worker_name:
                return True
        return False
    except Exception as e:
        print(f"Error checking worker names: {e}")
        return False

def enqueue_document(coordinator_url, file_path, schema_name=None):
    """Enqueue a single document for processing."""
    # If schema is specified, validate it exists first
    if schema_name and schema_name != "*":
        try:
            # Get list of available schemas
            schemas_response = requests.get(f"{coordinator_url}/api/schemas")
            if schemas_response.status_code != 200:
                return {"error": f"Failed to retrieve schemas: {schemas_response.text}"}

            schemas = schemas_response.json().get("schemas", [])
            schema_names = [schema.get("name") for schema in schemas]

            # Check if specified schema exists
            if schema_name not in schema_names:
                return {"error": f"Schema '{schema_name}' does not exist. Please add the schema first."}
        except Exception as e:
            return {"error": f"Error validating schema: {str(e)}"}

    # Prepare request params
    params = {"file_path": file_path}
    if schema_name:
        params["schema_name"] = schema_name

    response = requests.post(
        f"{coordinator_url}/api/enqueue",
        params=params
    )
    return response.json()

def enqueue_folder(coordinator_url, folder_path, schema_name=None):
    """Enqueue all documents in a folder."""
    # If schema is specified, validate it exists first
    if schema_name and schema_name != "*":
        try:
            # Get list of available schemas
            schemas_response = requests.get(f"{coordinator_url}/api/schemas")
            if schemas_response.status_code != 200:
                return {"error": f"Failed to retrieve schemas: {schemas_response.text}"}

            schemas = schemas_response.json().get("schemas", [])
            schema_names = [schema.get("name") for schema in schemas]

            # Check if specified schema exists
            if schema_name not in schema_names:
                return {"error": f"Schema '{schema_name}' does not exist. Please add the schema first."}
        except Exception as e:
            return {"error": f"Error validating schema: {str(e)}"}

    params = {"folder_path": folder_path}
    if schema_name:
        params["schema_name"] = schema_name

    response = requests.post(
        f"{coordinator_url}/api/enqueue-folder",
        params=params
    )
    return response.json()

# Update get_system_status function to include processed documents count
def get_system_status(coordinator_url):
    """Get current system status."""
    try:
        response = requests.get(f"{coordinator_url}/api/system-status")

        # Check if response was successful
        response.raise_for_status()

        # Try to parse JSON, handle empty response
        if not response.text.strip():
            print(f"Warning: Empty response from server")
            return {"error": "Empty response from server", "queue_status": {"pending": 0, "processing": 0, "processed": 0},
                    "workers": []}

        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"Error connecting to coordinator: {e}")
        # Return a default structure so the main code doesn't break
        return {"error": str(e), "queue_status": {"pending": 0, "processing": 0, "processed": 0}, "workers": []}
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
        return {"error": "Could not connect to coordinator server", "queue_status": {"pending": 0, "processing": 0, "processed": 0},
                "workers": []}
    except ValueError as e:
        print(f"Error parsing response: {e}")
        print(f"Response content: {response.text[:200]}..." if len(response.text) > 200 else response.text)
        return {"error": str(e), "queue_status": {"pending": 0, "processing": 0, "processed": 0}, "workers": []}

def get_worker_status(coordinator_url, worker_id):
    """Get status of a specific worker."""
    response = requests.get(f"{coordinator_url}/api/worker/{worker_id}")
    return response.json()

def start_worker(coordinator_url, worker_id):
    """Start a stopped worker by launching it with its existing properties."""
    print(f"Attempting to start worker: {worker_id}")

    # Check if worker exists and get its current properties
    try:
        worker_info = get_worker_status(coordinator_url, worker_id)
        if "error" in worker_info:
            print(f"Error: {worker_info['error']}")
            return worker_info

        worker_data = worker_info.get("worker", {})
        worker_status = worker_data.get("status")

        # Check if worker is in a stopped state
        if worker_status != "stopped" and worker_status != "error":
            error_msg = f"Worker is in '{worker_status}' state, not stopped. Cannot start."
            print(error_msg)
            return {"error": error_msg}

        # Get worker properties
        worker_name = worker_data.get("name")
        api_url = worker_data.get("api_url")
        model = worker_data.get("model")
        api_key = worker_data.get("api_key")

        print(f"Starting worker: {worker_name} with model: {model}")

        # Make API call to update worker status in Redis (from stopped to idle)
        response = requests.post(f"{coordinator_url}/api/worker/start/{worker_id}")
        if response.status_code != 200:
            print(f"Failed to update worker status: {response.text}")
            return {"error": f"Failed to update worker status: {response.text}"}

        # Create the helper script in the same directory
        helper_script_path = Path(__file__).parent / "existing_worker.py"

        # Build the command to run the helper script
        command = [
            sys.executable,
            str(helper_script_path),
            "--coordinator", coordinator_url,
            "--name", worker_name,
            "--api-url", api_url,
            "--model", model,
            "--worker-id", worker_id
        ]

        # Add API key if it exists
        if api_key:
            command.extend(["--api-key", api_key])

        # Run the worker in the current terminal
        print(f"Running worker: {' '.join(command)}")
        subprocess.call(command)

        return {
            "status": "Worker process completed",
            "name": worker_name,
            "id": worker_id,
            "model": model,
            "api_url": api_url
        }
    except KeyboardInterrupt:
        print("\nWorker stopped by user (Ctrl+C)")
        return {
            "status": "Worker stopped by user",
            "id": worker_id,
            "name": worker_name,
        }

    except Exception as e:
        print(f"Error starting worker process: {e}")
        return {"error": str(e)}

def stop_worker(coordinator_url, worker_id):
    """Stop a running worker."""
    print(f"Forcefully removing worker: {worker_id}")

    # Try to get worker info before removal (might fail if already removed)
    try:
        worker_info = get_worker_status(coordinator_url, worker_id)
        worker_name = worker_info.get("worker", {}).get("name")
        process_id = worker_info.get("worker", {}).get("process_id")
        if worker_name:
            print(f"Found worker name: {worker_name}")
        if process_id:
            print(f"Found process ID: {process_id}")
    except Exception as e:
        print(f"Could not retrieve worker info: {e}")
        worker_name = None
        process_id = None

    # Send request to remove worker from Redis
    try:
        response = requests.post(f"{coordinator_url}/api/worker/stop/{worker_id}")
        print(f"Stop response: {response.status_code} - {response.text}")
    except Exception as e:
        # print(f"Error during worker removal API call: {e}")
        response = {"error": str(e)}

    # Kill process based on OS
    print("Attempting to kill any matching processes...")

    if os.name == 'nt':  # Windows
        # Use the worker_id in window title searches regardless of worker info
        kill_commands = [
            # Kill by worker ID in title
            ["taskkill", "/F", "/FI", f"WINDOWTITLE *{worker_id}*"],
            # Kill all cmd windows with worker.py in title
            ["taskkill", "/F", "/FI", "WINDOWTITLE *worker.py*"],
            # Kill all python processes running worker.py
            ["wmic", "process", "where", "commandline like '%python%worker.py%'", "delete"]
        ]

        # Add specific commands if we have worker name or PID
        if worker_name:
            kill_commands.append(["taskkill", "/F", "/FI", f"WINDOWTITLE *{worker_name}*"])
        if process_id:
            kill_commands.append(["taskkill", "/F", "/T", "/PID", str(process_id)])

        # Execute all kill commands
        for cmd in kill_commands:
            try:
                # print(f"Executing: {' '.join(cmd)}")
                result = subprocess.run(cmd, capture_output=True, text=True)
            except Exception as e:
                print(f"Command failed: {e}")

    else:  # Unix-based (macOS, Linux)
        kill_commands = []

        # Kill by PID if available
        if process_id:
            kill_commands.append(["kill", "-9", str(process_id)])

        # Kill by worker name pattern
        if worker_name:
            kill_commands.append(["pkill", "-f", f"python.*worker.py.*{worker_name}"])

        # Kill by worker_id pattern
        kill_commands.append(["pkill", "-f", f"python.*worker.py.*{worker_id}"])

        # Kill any worker.py processes as last resort
        kill_commands.append(["pkill", "-f", "python.*worker.py"])

        # Execute all kill commands
        for cmd in kill_commands:
            try:
                # print(f"Executing: {' '.join(cmd)}")
                result = subprocess.run(cmd, capture_output=True, text=True)
                print(f"Command output: {result.stdout.strip() if result.stdout else 'No output'}")
            except Exception as e:
                print(f"Command failed: {e}")

    print("Worker force removal completed")
    return response.json() if hasattr(response, 'json') else response

def remove_worker(coordinator_url, worker_id):
    """Forcefully terminate and remove a worker."""
    print(f"Forcefully removing worker: {worker_id}")

    # Try to get worker info before removal (might fail if already removed)
    try:
        worker_info = get_worker_status(coordinator_url, worker_id)
        worker_name = worker_info.get("worker", {}).get("name")
        process_id = worker_info.get("worker", {}).get("process_id")
        if worker_name:
            print(f"Found worker name: {worker_name}")
        if process_id:
            print(f"Found process ID: {process_id}")
    except Exception as e:
        print(f"Could not retrieve worker info: {e}")
        worker_name = None
        process_id = None

    # Send request to remove worker from Redis
    try:
        response = requests.delete(f"{coordinator_url}/api/force-remove-worker/{worker_id}")
        print(f"Removal response: {response.status_code} - {response.text}")
    except Exception as e:
        # print(f"Error during worker removal API call: {e}")
        response = {"error": str(e)}

    # Kill process based on OS
    print("Attempting to kill any matching processes...")

    if os.name == 'nt':  # Windows
        # Use the worker_id in window title searches regardless of worker info
        kill_commands = [
            # Kill by worker ID in title
            ["taskkill", "/F", "/FI", f"WINDOWTITLE *{worker_id}*"],
            # Kill all cmd windows with worker.py in title
            ["taskkill", "/F", "/FI", "WINDOWTITLE *worker.py*"],
            # Kill all python processes running worker.py
            ["wmic", "process", "where", "commandline like '%python%worker.py%'", "delete"]
        ]

        # Add specific commands if we have worker name or PID
        if worker_name:
            kill_commands.append(["taskkill", "/F", "/FI", f"WINDOWTITLE *{worker_name}*"])
        if process_id:
            kill_commands.append(["taskkill", "/F", "/T", "/PID", str(process_id)])

        # Execute all kill commands
        for cmd in kill_commands:
            try:
                # print(f"Executing: {' '.join(cmd)}")
                result = subprocess.run(cmd, capture_output=True, text=True)
            except Exception as e:
                print(f"Command failed: {e}")

    else:  # Unix-based (macOS, Linux)
        kill_commands = []

        # Kill by PID if available
        if process_id:
            kill_commands.append(["kill", "-9", str(process_id)])

        # Kill by worker name pattern
        if worker_name:
            kill_commands.append(["pkill", "-f", f"python.*worker.py.*{worker_name}"])

        # Kill by worker_id pattern
        kill_commands.append(["pkill", "-f", f"python.*worker.py.*{worker_id}"])

        # Kill any worker.py processes as last resort
        kill_commands.append(["pkill", "-f", "python.*worker.py"])

        # Execute all kill commands
        for cmd in kill_commands:
            try:
                # print(f"Executing: {' '.join(cmd)}")
                result = subprocess.run(cmd, capture_output=True, text=True)
                print(f"Command output: {result.stdout.strip() if result.stdout else 'No output'}")
            except Exception as e:
                print(f"Command failed: {e}")

    print("Worker force removal completed")
    return response.json() if hasattr(response, 'json') else response

def start_new_worker(worker_name, coordinator_url, model, api_url, api_key=None):
    """Start a new worker process in the current terminal."""

    # Check if worker name already exists
    if worker_name_exists(coordinator_url, worker_name):
        print(f"Error: Worker with name '{worker_name}' already exists")
        return {"error": f"Worker with name '{worker_name}' already exists"}

    print(f"Starting new worker: {worker_name}")

    # Build the command with proper arguments
    worker_script = str(Path(__file__).parent / "worker.py")
    command = [
        sys.executable,  # Python interpreter
        worker_script,
        "--coordinator", coordinator_url,
        "--name", worker_name,
        "--api-url", api_url,
        "--model", model
    ]

    # Add API key if provided
    if api_key:
        command.extend(["--api-key", api_key])

    try:
        # Run the process directly in the current terminal
        print(f"Running worker: {' '.join(command)}")
        # Using subprocess.call to block until the process completes
        subprocess.call(command)

        return {
            "status": "Worker process completed",
            "name": worker_name,
            "model": model,
            "api_url": api_url
        }
    except KeyboardInterrupt:
        print("\nWorker stopped by user")
        return {
            "status": "Worker stopped by user",
            "name": worker_name,
            "model": model,
            "api_url": api_url
        }
    except Exception as e:
        print(f"Error starting worker process: {e}")
        return {"error": str(e)}

def add_schema(coordinator_url, schema_name, schema_content):
    """Add a schema to the system."""
    # Check if schema already exists
    try:
        existing_schemas = list_schemas(coordinator_url)
        if schema_name in existing_schemas:
            return {"error": f"Schema with name '{schema_name}' already exists"}

        # Validate JSON
        if not isinstance(schema_content, dict):
            return {"error": "Invalid JSON format"}

        # Proceed with adding schema
        response = requests.post(
            f"{coordinator_url}/api/schema",
            json={"name": schema_name, "content": schema_content}
        )
        return response.json()
    except Exception as e:
        return {"error": f"Failed to add schema: {str(e)}"}

def list_schemas(coordinator_url):
    """List all available schemas."""
    response = requests.get(f"{coordinator_url}/api/schemas")
    data = response.json()

    # Extract only schema names from the response
    schema_names = [schema["name"] for schema in data.get("schemas", [])]

    return schema_names

def get_schema(coordinator_url, schema_name):
    """Get a specific schema by name."""
    try:
        response = requests.get(f"{coordinator_url}/api/schema/{schema_name}")

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404 or "not found" in response.text.lower():
            return {"error": f"Schema '{schema_name}' not found"}
        else:
            return {"error": f"Failed to retrieve schema: HTTP {response.status_code}"}
    except Exception as e:
        return {"error": f"Failed to retrieve schema: {str(e)}"}

def delete_schema(coordinator_url, schema_name):
    """Delete a schema by name."""
    try:
        # Check if schema exists
        existing_schemas = list_schemas(coordinator_url)
        if schema_name not in existing_schemas:
            return {"error": f"Schema not found: '{schema_name}'"}

        # If it exists, proceed with deletion
        response = requests.delete(f"{coordinator_url}/api/schema/{schema_name}")
        return response.json()
    except Exception as e:
        return {"error": f"Failed to delete schema: {str(e)}"}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Document Processing Client")
    parser.add_argument("--coordinator", default="http://localhost:8000", help="Coordinator URL")

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Enqueue document command
    enqueue_parser = subparsers.add_parser("enqueue", help="Enqueue a document")
    enqueue_parser.add_argument("file_path", help="Path to document file")
    enqueue_parser.add_argument("-s", "--schema", help="Schema name to use for processing")
    # Enqueue folder command
    folder_parser = subparsers.add_parser("enqueue-folder", help="Enqueue all documents in a folder")
    folder_parser.add_argument("folder_path", help="Path to folder containing documents")
    folder_parser.add_argument("-s", "--schema", help="Schema name to use for processing")

    # Status command
    status_parser = subparsers.add_parser("status", help="Get system status")

    # Worker commands - main worker subparser
    worker_parser = subparsers.add_parser("worker", help="Worker operations")
    worker_subparsers = worker_parser.add_subparsers(dest="worker_command", help="Worker command to execute")
    # Worker status command
    worker_status_parser = worker_subparsers.add_parser("status", help="Get worker status")
    worker_status_parser.add_argument("worker_id", help="ID of worker")
    # Stop worker command
    stop_parser = worker_subparsers.add_parser("stop", help="Stop a worker")
    stop_parser.add_argument("worker_id", help="ID of worker to stop")
    # Start worker command
    start_parser = worker_subparsers.add_parser("start", help="Start a stopped worker")
    start_parser.add_argument("worker_id", help="ID of worker to start")
    # Remove worker command
    remove_parser = worker_subparsers.add_parser("delete", help="Remove a worker")
    remove_parser.add_argument("worker_id", help="ID of worker to remove")
    # New worker command
    new_worker_parser = worker_subparsers.add_parser("new", help="Start a new worker")
    new_worker_parser.add_argument("--name", default=f"worker-{os.urandom(4).hex()}", help="Worker name")
    new_worker_parser.add_argument("--model", default="gpt-4o-mini", help="LLM model name")
    new_worker_parser.add_argument("--api-url", default="https://api.openai.com/v1/chat/completions",
                                help="LLM API URL")
    new_worker_parser.add_argument("--api-key", default="", help="LLM API key")

    # In the argument parser section, replace the existing schema parsers with:
    schema_parser = subparsers.add_parser("schema", help="Schema operations")
    schema_subparsers = schema_parser.add_subparsers(dest="schema_command", help="Schema command to execute")
    # Schema new command (was add-schema)
    schema_new_parser = schema_subparsers.add_parser("new", help="Add a new schema to the system")
    schema_new_parser.add_argument("schema_name", help="Name of the schema")
    schema_new_parser.add_argument("-f", "--file", help="Path to schema JSON file")
    schema_new_parser.add_argument("-c", "--content", help="Schema content as JSON string")
    # Schema list command (was list-schemas)
    schema_list_parser = schema_subparsers.add_parser("list", help="List all available schemas")
    # Schema show command (was show-schema)
    schema_show_parser = schema_subparsers.add_parser("show", help="Show schema content")
    schema_show_parser.add_argument("schema_name", help="Name of the schema to display")
    # Schema delete command (was delete-schema)
    schema_delete_parser = schema_subparsers.add_parser("delete", help="Delete a schema")
    schema_delete_parser.add_argument("schema_name", help="Name of the schema to delete")

    args = parser.parse_args()

    if args.command == "enqueue":
        result = enqueue_document(args.coordinator, args.file_path, args.schema)
        print(json.dumps(result, indent=2))
        # Replace the schema command handling conditions with:
    elif args.command == "schema":
        if args.schema_command == "new":
            schema_content = None

            if args.file:
                try:
                    with open(args.file, 'r', encoding='utf-8') as f:
                        schema_content = json.load(f)
                except Exception as e:
                    print(f"Error reading schema file: {e}")
                    sys.exit(1)
            elif args.content:
                try:
                    content = args.content
                    # Remove outer quotes if they exist and replace single quotes with double quotes
                    try:
                        schema_content = json.loads(content)
                    except json.JSONDecodeError:
                        # If direct loading fails, replace single quotes with double quotes
                        # for JSON keys and values (common shell quoting issue)
                        content = content.replace("'", '"')
                        schema_content = json.loads(content)

                except json.JSONDecodeError as e:
                    print(f"Error: Invalid JSON content - {str(e)}")
                    print("Make sure your JSON is properly formatted with double quotes around keys and string values")
                    sys.exit(1)
            else:
                print("Error: Either --file or --content must be specified")
                sys.exit(1)

            result = add_schema(args.coordinator, args.schema_name, schema_content)
            if "error" in result:
                print(f"Error: {result['error']}")
            else:
                print(f"Schema '{args.schema_name}' added successfully")
        elif args.schema_command == "list":
            schemas = list_schemas(args.coordinator)
            print("\n".join(schemas))
        elif args.schema_command == "show":
            schema_result = get_schema(args.coordinator, args.schema_name)

            if "error" in schema_result:
                print(f"Error: {schema_result['error']}")
            else:
                # Pretty-print the schema content
                content = schema_result.get("content", {})
                if content:
                    print(json.dumps(content, indent=2))
                else:
                    print("Schema exists but has no content")
        elif args.schema_command == "delete":
            result = delete_schema(args.coordinator, args.schema_name)
            if "error" in result:
                print(f"Error: {result['error']}")
            else:
                print(f"Schema '{args.schema_name}' deleted successfully")
        else:
            schema_parser.print_help()
    elif args.command == "enqueue-folder":
        result = enqueue_folder(args.coordinator, args.folder_path, args.schema)
        print(json.dumps(result, indent=2))
        # Update the status command display code
    elif args.command == "status":
        status = get_system_status(args.coordinator)
        proceseed_count=status['queue_status'].get('processed', 0)
        errors_count=status['queue_status'].get('errors', 0)
        success_count = proceseed_count - errors_count
        print("\nSystem Status:")
        print(f"Documents:")
        print(f"  • Pending:    {status['queue_status']['pending']}")
        print(f"  • Processing: {status['queue_status']['processing']}")
        print(f"  • Processed:  {proceseed_count}"
              f" --> {success_count} Success, {errors_count} Errors")
        print("\nWorkers:")
        for worker in status['workers']:
            print(f"  • {worker['name']} ({worker['id']}): {worker['status']}")
    elif args.command == "worker":
        if args.worker_command == "status":
            status = get_worker_status(args.coordinator, args.worker_id)
            if "error" in status:
                print(f"Error: {status['error']}")
            else:
                print(f"\nWorker: {status['worker']['name']} ({status['worker']['id']})")
                print(f"Status: {status['worker']['status']}")
                print(f"Model: {status['worker']['model']}")
                print(f"Processed: {status['stats']['processed_documents']} documents")
                print(f"Errors: {status['stats']['errors']}")
        elif args.worker_command == "stop":
            result = stop_worker(args.coordinator, args.worker_id)
            print(f"Worker stop: {result}")
        elif args.worker_command == "start":
            result = start_worker(args.coordinator, args.worker_id)
            print(f"{result}")
        elif args.worker_command == "delete":
            result = remove_worker(args.coordinator, args.worker_id)
            print(f"Worker removal: {result}")
        elif args.worker_command == "new":
            if args.api_key is None and "openai.com" in args.api_url:
                print("WARNING: Using OpenAI API without providing an API key")

            result = start_new_worker(
                worker_name=args.name,
                coordinator_url=args.coordinator,
                model=args.model,
                api_url=args.api_url,
                api_key=args.api_key
            )
            # print(f"New worker: {result}")
        else:
            worker_parser.print_help()
    else:
        parser.print_help()