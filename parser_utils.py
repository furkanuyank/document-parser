import os
import json
from extractor import Extractor
from json_utils import extract_json_from_text, validate, merge_json_list
from prompt_utils import prompt_generator, select_schema


def _serve_result(results, num_pages, query, file_path, model):
    # Convert results to a serializable format
    if isinstance(results, dict):
        # Handle any sets in the dictionary
        json_result = convert_sets_to_lists(results)
    elif isinstance(results, list):
        # Process list of results
        json_result = [convert_sets_to_lists(item) if isinstance(item, dict) else item for item in results]
    else:
        # Extract JSON from text
        json_result = extract_json_from_text(results)

    # If json_result is still a list, merge it into a single dict
    if isinstance(json_result, list):
        json_result = merge_json_list(json_result)

    # Add metadata
    if isinstance(json_result, dict):
        json_result.update({
            "meta": {
                "num_pages": num_pages,
                "query": query,
                "file": os.path.basename(file_path),
                "model": model
            }
        })

    return json_result


def convert_sets_to_lists(d):
    """Convert any sets in a dictionary to lists recursively."""
    result = {}
    for key, value in d.items():
        if isinstance(value, set):
            result[key] = list(value)
        elif isinstance(value, dict):
            result[key] = convert_sets_to_lists(value)
        elif isinstance(value, list):
            result[key] = [
                convert_sets_to_lists(item) if isinstance(item, dict)
                else list(item) if isinstance(item, set)
                else item for item in value
            ]
        else:
            result[key] = value
    return result


def run_parser(file_path, api_url, model, api_key, query=None, type=None, schema=None):
    if not os.path.exists(file_path):
        return {"error": f"Dosya bulunamadı: {file_path}"}

    extractor = Extractor()

    if schema == "*" or schema is None:
        query_text = prompt_generator(type, query)
    else:
        selected_schema = select_schema(schema)
        query_text = prompt_generator(type, selected_schema)

    input_data = [
        {
            "file_path": file_path,
            "text_input": query_text
        }
    ]

    results, num_pages = extractor.run_inference(
        api_url,
        model,
        api_key,
        input_data,
    )

    return _serve_result(results, num_pages, query, file_path, model)