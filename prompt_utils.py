# prompt_utils.py
import json
import os
from string import Template
import redis
from redis import ConnectionPool

# Initialize connection pool at module level
REDIS_POOL = ConnectionPool(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True,
    max_connections=5,  # Fewer connections needed for prompt utils
    socket_timeout=5,
    socket_connect_timeout=2
)

SCHEMAS_SET = "available_schemas"
SCHEMA_DIR = "./schemas"

TEMPLATES = {
    "extract_fields": Template(
        """You are a document analysis assistant. Analyze the image and extract the following information:
$fields

Return the information in valid JSON format with the specified fields. Use null for missing or unclear fields.
"""
    ),
    "extract_schema": Template(
        """You are a document analysis assistant. Analyze the image and extract the schema of the data.
        Schema:
        $schema

Return the schema in valid JSON format. Use null for missing or unclear fields.
"""
    ),
    "general_extraction": Template(
        """You are a document analysis assistant. Analyze the document and extract all relevant information.
Return your analysis as a structured JSON with appropriate fields and values.
Use null for missing or unclear information. Response in Turkish
"""
    ),
    "classification": Template(
        """You are a document analysis assistant. Classify the document into one of the following categories:
    - Invoice
    - Contract
    - Report
    - Other

    Return the classification as a single word.
    """
    )
}


def get_prompt_template(template_name="general_extraction", **kwargs):
    if template_name not in TEMPLATES:
        return TEMPLATES["general_extraction"].safe_substitute(**kwargs)

    return TEMPLATES[template_name].safe_substitute(**kwargs)


def prompt_generator(type, query):
    if type is None or type == "*" or (type == "schema" and query == "*"):
        return get_prompt_template(template_name="general_extraction")
    elif type == "classification":
        return get_prompt_template(template_name="classification")
    elif type == "field":
        try:
            fields = [field.strip() for field in query.split(",")]
            fields_text = "\n".join(f"- {field}" for field in fields)
            return get_prompt_template(template_name="extract_fields", fields=fields_text)
        except json.JSONDecodeError:
            return query
    elif type == "schema":
        return get_prompt_template(template_name="extract_schema", schema=query)

    return get_prompt_template()


def select_schema(name):
    """Retrieve schema from Redis by name or from filesystem as fallback."""
    try:
        # Use connection pool instead of creating new connection
        redis_client = redis.Redis(connection_pool=REDIS_POOL)

        # Check if schema exists in Redis with timeout
        schema_exists = redis_client.sismember(SCHEMAS_SET, name)
        if schema_exists:
            # Get schema content from Redis with pipeline for efficiency
            pipe = redis_client.pipeline()
            pipe.hgetall(f"schema:{name}")
            results = pipe.execute()
            schema_data = results[0] if results else {}

            if schema_data and "content" in schema_data:
                # Parse the schema content from JSON string
                return json.loads(schema_data["content"])

        # If schema not found in Redis, try filesystem as fallback
        schema_path = os.path.join(SCHEMA_DIR, f"{name}.json")
        if os.path.exists(schema_path):
            with open(schema_path, "r", encoding="utf-8") as schema_file:
                return json.load(schema_file)

        # If schema not found in either location
        raise ValueError(f"Şema bulunamadı: '{name}' şeması Redis'te veya dosya sisteminde bulunamadı")

    except redis.RedisError as re:
        raise ValueError(f"Redis bağlantı hatası: {str(re)}")
    except json.JSONDecodeError as je:
        raise ValueError(f"Şema JSON hatası '{name}': {str(je)}")
    except Exception as e:
        raise ValueError(f"Şema yükleme hatası '{name}': {str(e)}")