import os
import json
import argparse
from vllm_extractor import VLLMExtractor
from json_utils import extract_json_from_text, validate, merge_json_list
from prompt_templates import prompt_generator
from dotenv import load_dotenv

def run_parser(file_path, api_url, query=None, type=None):
    if not os.path.exists(file_path):
        return {"error": f"Dosya bulunamadı: {file_path}"}
        
    extractor = VLLMExtractor()
    query_text = prompt_generator(type, query)
            
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

    processed_results = []
    for result in results:
        try:
            print("result: "+str(result))
            json_result=extract_json_from_text(result)
            print("json_result: "+str(json_result))

            processed_results.append(json_result)
        except ValueError as e:
            print(f"JSON ayrıştırma hatası: {e}")
            processed_results.append({"error": str(e)})

    merged_result= merge_json_list(processed_results)
    keys,types=validate(merged_result,query,type)

    # TODO: schema ya * gelirse veya field gelirse null dön validler için
    response = {
        "file_path": file_path,
        "num_pages": num_pages,
        "is_keys_valid": keys,
        "is_types_valid": types,
        "result": merged_result
    }
    
    return response

# TODO: birden fazla istek için json birleştir
# TODO: null değeri, string number vs kabul edilsin mi (şuan ediliyor)
def main():
    parser = argparse.ArgumentParser(description='Belge işleme ve veri çıkarma aracı')
    # parser.add_argument('file', help='İşlenecek dosya yolu')
    # parser.add_argument('--api-url', required=True, help='VLLM API URL\'si')
    parser.add_argument('--query', default='*', help='Sorgu metni veya JSON şeması')
    parser.add_argument('--type', default='Schema', help='Sorgu metni veya JSON şeması')
    parser.add_argument('--output', help='Çıktı JSON dosyası (belirtilmezse stdout kullanılır)')
    
    args = parser.parse_args()
    
    result = run_parser(
        "deneme.png",
        "https://api.openai.com/v1/chat/completions",
        type='field',
        query='''document_number:string,date:string'''
    )
    
    output_json = json.dumps(result, indent=2, ensure_ascii=False)
    print(output_json)
  
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    print("API anahtarı tanımlı değil.")
    
if __name__ == "__main__":
    main()