import json
import os
from string import Template

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

def prompt_generator(type,query):
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
    schema_path = os.path.join(SCHEMA_DIR, f"{name}.json")
    
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Şema bulunamadı: {schema_path}")
    
    with open(schema_path, "r", encoding="utf-8") as schema_file:
        schema = json.load(schema_file)
    
    return schema