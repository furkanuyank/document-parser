import os
import json
import shutil
import tempfile
import requests
import base64
from PIL import Image
import logging
import re

from pdf_optimizer import PDFOptimizer
from image_optimizer import ImageOptimizer
from prompt_templates import get_prompt_template
from table_structure_processor import TableDetector

class VLLMExtractor:
    """VLLM API ile belge analizi yapan sınıf."""
    
    def run_inference(self, api_url, input_data, tables_only=False, 
                     generic_query=False, crop_size=0, debug_dir=None, debug=False):
        """
        VLLM API kullanarak belge analizi yapar.
        
        Args:
            api_url: VLLM API'nin URL'si.
            input_data: Girdi verileri.
            tables_only: Sadece tabloları işle.
            generic_query: Genel sorgu kullan.
            crop_size: Görüntü kırpma boyutu.
            debug_dir: Hata ayıklama çıktıları için klasör.
            debug: Hata ayıklama bayrağı.
            
        Returns:
            tuple: (sonuçlar listesi, sayfa sayısı)
        """
        if not input_data or not input_data[0].get("file_path"):
            return [], 0
            
        file_path = input_data[0]["file_path"]
        
        # Dosya uzantısına göre işleme türünü belirle
        if file_path.lower().endswith('.pdf'):
            return self._process_pdf(api_url, input_data, tables_only, crop_size, debug, debug_dir)
        else:
            return self._process_non_pdf(api_url, input_data, tables_only, crop_size, debug, debug_dir)
            
    def _process_pdf(self, api_url, input_data, tables_only, crop_size, debug, debug_dir):
        """
        PDF dosyalarını işler ve analiz yapar.
        
        Args:
            api_url: VLLM API URL'si.
            input_data: Girdi verileri.
            tables_only: Sadece tabloları işle.
            crop_size: Görüntü kırpma boyutu.
            debug: Hata ayıklama bayrağı.
            debug_dir: Hata ayıklama çıktıları için klasör.
            
        Returns:
            tuple: (sonuçlar listesi, sayfa sayısı)
        """
        pdf_optimizer = PDFOptimizer()
        num_pages, output_files, temp_dir = pdf_optimizer.split_pdf_to_pages(
            input_data[0]["file_path"],
            debug_dir, 
            convert_to_images=True
        )

        results = self._process_pages(api_url, output_files, input_data, tables_only, crop_size, debug, debug_dir)

        # Geçici klasörü temizle
        shutil.rmtree(temp_dir, ignore_errors=True)
        return results, num_pages
        
    def _process_non_pdf(self, api_url, input_data, tables_only, crop_size, debug, debug_dir):
        """
        PDF olmayan dosyaları işler ve analiz yapar.
        
        Args:
            api_url: VLLM API URL'si.
            input_data: Girdi verileri.
            tables_only: Sadece tabloları işle.
            crop_size: Görüntü kırpma boyutu.
            debug: Hata ayıklama bayrağı.
            debug_dir: Hata ayıklama çıktıları için klasör.
            
        Returns:
            tuple: (sonuçlar listesi, sayfa sayısı)
        """
        file_path = input_data[0]["file_path"]

        if tables_only:
            return self._extract_tables(api_url, file_path, input_data, debug, debug_dir), 1
        else:
            temp_dir = tempfile.mkdtemp()

            if crop_size:
                if debug:
                    print(f"Görüntü kenarları {crop_size} piksel kırpılıyor.")
                image_optimizer = ImageOptimizer()
                cropped_file_path = image_optimizer.crop_image_borders(file_path, temp_dir, debug_dir, crop_size)
                input_data[0]["file_path"] = cropped_file_path

            file_path = input_data[0]["file_path"]
            result = self._call_vllm_api(api_url, file_path, input_data[0].get("text_input", ""))

            shutil.rmtree(temp_dir, ignore_errors=True)

            return [result], 1
            
    def _process_pages(self, api_url, page_files, input_data, tables_only, crop_size, debug, debug_dir):
        """
        Sayfaları işler ve analiz yapar.
        
        Args:
            api_url: VLLM API URL'si.
            page_files: Sayfa dosyalarının listesi.
            input_data: Girdi verileri.
            tables_only: Sadece tabloları işle.
            crop_size: Görüntü kırpma boyutu.
            debug: Hata ayıklama bayrağı.
            debug_dir: Hata ayıklama çıktıları için klasör.
            
        Returns:
            list: Sonuçlar listesi.
        """
        results = []
        
        # Orijinal sorguyu al
        text_input = input_data[0].get("text_input", "")
        
        for page_file in page_files:
            if tables_only:
                page_result = self._extract_tables(api_url, page_file, input_data, debug, debug_dir)
            else:
                # Görüntü kırpma gerekiyorsa
                if crop_size:
                    temp_dir = tempfile.mkdtemp()
                    image_optimizer = ImageOptimizer()
                    cropped_file_path = image_optimizer.crop_image_borders(page_file, temp_dir, debug_dir, crop_size)
                    page_file = cropped_file_path
                
                # VLLM API çağrısı
                page_result = self._call_vllm_api(api_url, page_file, text_input)
                
                if crop_size:
                    shutil.rmtree(temp_dir, ignore_errors=True)
            
            results.append(page_result)
        
        return results
        
    def _extract_tables(self, api_url, file_path, input_data, debug, debug_dir):
        """
        Görüntüdeki tabloları çıkarır ve analiz eder.
        
        Args:
            api_url: VLLM API URL'si.
            file_path: Dosya yolu.
            input_data: Girdi verileri.
            debug: Hata ayıklama bayrağı.
            debug_dir: Hata ayıklama çıktıları için klasör.
            
        Returns:
            dict: Tablo içerikleri.
        """
        # Tabloları algıla
        table_detector = TableDetector()
        table_images = table_detector.detect_tables(file_path, local=True, debug_dir=debug_dir, debug=debug)
        
        if not table_images:
            if debug:
                print(f"Hiçbir tablo algılanamadı: {file_path}")
            return {"tables": []}
            
        # Her tabloda içerik çıkar
        tables = []
        for i, table_img in enumerate(table_images):
            # Geçici dosyaya kaydet
            temp_dir = tempfile.mkdtemp()
            table_path = os.path.join(temp_dir, f"table_{i}.png")
            table_img.save(table_path)
            
            # VLLM API çağrısı
            table_query = get_prompt_template("extract_tables")
            table_content = self._call_vllm_api(api_url, table_path, table_query)
            
            # Geçici dosyayı temizle
            shutil.rmtree(temp_dir, ignore_errors=True)
            
            tables.append({
                "table_index": i,
                "content": table_content
            })
            
        return {"tables": tables}
        
    def _call_vllm_api(self, api_url, file_path, prompt):
        print(prompt+"------\n")
        """
        VLLM API'ye istek gönderir.
        
        Args:
            api_url: VLLM API URL'si.
            file_path: Dosya yolu.
            prompt: Sorgu metni.
            
        Returns:
            dict: API yanıtı.
        """
        try:
            # Dosyayı base64'e dönüştür
            with open(file_path, "rb") as f:
                image_base64 = base64.b64encode(f.read()).decode("utf-8")
            # print(image_base64)

            headers = {
            "Content-Type": "application/json"
            }           
            data = {
            "model": "Qwen/Qwen2.5-VL-7B-Instruct-AWQ",
            "messages": [
                {
                    "role": "user",
                    "content": [
                    {
                    "type": "text",
                    "text": """You are a document analysis assistant. Analyze the image and extract the following information:
gönderen, alıcı, ETTN, seneryo

Return the information in valid JSON format with the specified fields. Use null for missing or unclear fields.
"""
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{image_base64}"
                    }
                }
            ]
        }
    ]
}
            
            # API isteği payload'ını hazırla
            # data = {
            #     "model": "Qwen/Qwen2.5-VL-7B-Instruct-AWQ",  # VLLM'in kullandığı model adı
            #     "messages":messages,
            #     # "prompt": prompt,
            #     # "images": [image_base64],
            #     # "max_tokens": 1500,
            #     "temperature": 0.2,
            #     # "stream": False
            # }
            
            # API isteği gönder
            response = requests.post(api_url, json=data,headers=headers)
            response_json=response.json()
            ai_response_raw = response_json["choices"][0]["message"]["content"]
            print(ai_response_raw)
            print("-------------------\n")
            response.raise_for_status()
            
#             match = re.search(r"```json\s*(.*?)\s*```", ai_response_raw, re.DOTALL)
#             if match:
#                 ai_response_cleaned = match.group(1)
#     # JSON'a parse et
#                 ai_result = json.loads(ai_response_cleaned)
#             else:
#                 ai_result = ai_response_raw  # Eğer düzgün formatta değilse ham hali

# # Sonucu yazdır
#             print(json.dumps(ai_result, indent=2, ensure_ascii=False))
            
            # JSON çıktısını bul ve parse et
            # try:
            #     json_match = re.search(r'```json\s*([\s\S]*?)\s*```', text_output)
            #     if json_match:
            #         json_str = json_match.group(1).strip()
            #         return json.loads(json_str)
            #     else:
            #         # JSON formatında değilse, metin olarak döndür
            #         return {"raw_text": text_output}
            # except (json.JSONDecodeError, re.error):
            #     return {"raw_text": text_output}
                
        except Exception as e:
            raise e
            logging.error(f"VLLM API çağrısı hatası: {e}")
            return {"error": str(e)}
