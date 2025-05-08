import os
import json
from vllm_extractor import VLLMExtractor
from gpt_extractor import GPTExtractor
from json_utils import extract_json_from_text, validate, merge_json_list
from prompt_utils import prompt_generator, select_schema


def _serve_result(results, num_pages, query, file_path):
    processed_results = []
    for result in results:
        try:
            json_result = extract_json_from_text(result)

            processed_results.append(json_result)
        except ValueError as e:
            print(f"JSON ayrıştırma hatası: {e}")
            processed_results.append({"error": str(e)})

    merged_result = merge_json_list(processed_results)
    keys, types = validate(merged_result, query, type)

    response = {
        "file_path": file_path,
        "num_pages": num_pages,
        "is_keys_valid": keys,
        "is_types_valid": types,
        "result": merged_result
    }

    return response

def run_parser_vllm(file_path, api_url, query=None, type=None, schema=None):
    if not os.path.exists(file_path):
        return {"error": f"Dosya bulunamadı: {file_path}"}
    
    extractor = VLLMExtractor()

    if schema=="*" or schema is None:
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
        input_data, 
    )

    return _serve_result(results, num_pages, query, file_path)
def run_parser_gpt(file_path, api_url, query=None, type=None, schema=None):
    if not os.path.exists(file_path):
        return {"error": f"Dosya bulunamadı: {file_path}"}
    
    extractor = GPTExtractor()

    if schema=="*" or schema is None:
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
        input_data, 
    )

    return _serve_result(results, num_pages, query, file_path)