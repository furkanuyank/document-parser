import os
import json
import argparse
from vllm_extractor import VLLMExtractor
from json_validator import JSONValidator

def run_parser(file_path, api_url, query=None, tables_only=False, crop_size=0, debug=False):
    """
    Belge işleme ve veri çıkarma işlevini yürütür.
    
    Args:
        file_path (str): İşlenecek dosyanın yolu.
        api_url (str): VLLM API URL'si.
        query (str, optional): Sorgu metni veya JSON şeması.
        tables_only (bool): Sadece tabloları işle.
        crop_size (int): Görüntü kırpma boyutu.
        debug (bool): Hata ayıklama bayrağı.
        
    Returns:
        dict: İşleme sonuçları.
    """
    # Dosya kontrolü
    if not os.path.exists(file_path):
        return {"error": f"Dosya bulunamadı: {file_path}"}
        
    # Hata ayıklama klasörü
    debug_dir = None
    if debug:
        debug_dir = os.path.join(os.path.dirname(file_path), "debug")
        os.makedirs(debug_dir, exist_ok=True)
        
    # Extractor oluştur
    extractor = VLLMExtractor()
    
    # Sorgu metni
    if query is None or query == "*":
        query_text = "Belgedeki tüm bilgileri çıkar ve JSON formatında döndür."
    else:
        # Query bir şema ise, metin sorguya dönüştür
        try:
            query_schema = json.loads(query) if isinstance(query, str) else query
            fields_text = "\n".join(f"- {field}" for field in query_schema.keys())
            query_text = f"Bu belgedeki şu bilgileri çıkar:\n{fields_text}\n\nYanıtı JSON formatında döndür."
        except json.JSONDecodeError:
            query_text = query
            
    # Girdi verilerini hazırla
    input_data = [
        {
            "file_path": file_path,
            "text_input": query_text
        }
    ]
    
    # İşlemeyi yap
    results, num_pages = extractor.run_inference(
        api_url, 
        input_data, 
        tables_only=tables_only,
        crop_size=crop_size, 
        debug_dir=debug_dir, 
        debug=debug
    )
    
    # Sonuçları doğrula
    if query != "*" and isinstance(query, str) and not tables_only:
        try:
            validator = JSONValidator(json.loads(query))
            for i, result in enumerate(results):
                validation_error = validator.validate_json_against_schema(result, validator.generated_schema)
                if validation_error and debug:
                    print(f"Sayfa {i+1} doğrulama hatası: {validation_error}")
        except json.JSONDecodeError:
            pass  # Geçersiz şema, doğrulama atlanır
    
    # Sonuçları biçimlendir
    response = {
        "file_path": file_path,
        "num_pages": num_pages,
        "results": results
    }
    
    return response

def main():
    """CLI için ana fonksiyon."""
    parser = argparse.ArgumentParser(description='Belge işleme ve veri çıkarma aracı')
    # parser.add_argument('file', help='İşlenecek dosya yolu')
    # parser.add_argument('--api-url', required=False, help='VLLM API URL\'si')
    parser.add_argument('--query', default='*', help='Sorgu metni veya JSON şeması')
    parser.add_argument('--tables-only', action='store_true', help='Sadece tabloları işle')
    parser.add_argument('--crop', type=int, default=0, help='Görüntü kırpma boyutu')
    parser.add_argument('--debug', action='store_true', help='Hata ayıklama modunu etkinleştir')
    parser.add_argument('--output', help='Çıktı JSON dosyası (belirtilmezse stdout kullanılır)')
    
    args = parser.parse_args()
    
    # Parser'ı çalıştır
    result = run_parser(
        "fatura.png",
        "http://localhost:8000/v1/chat/completions",
        query=args.query,
        tables_only=args.tables_only,
        crop_size=args.crop,
        debug=args.debug
    )
    
    # Sonuçları yazdır veya dosyaya kaydet
    output_json = json.dumps(result, indent=2, ensure_ascii=False)
    
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(output_json)
        print(f"Sonuçlar {args.output} dosyasına kaydedildi.")
    else:
        print(output_json)

if __name__ == "__main__":
    main()

    
