import os
import shutil
import requests
import base64

from pdf_optimizer import PDFOptimizer


class Extractor:

    def run_inference(self, api_url, model, api_key, input_data):
        if not input_data or not input_data[0].get("file_path"):
            return [], 0

        file_path = input_data[0]["file_path"]

        if file_path.lower().endswith('.pdf'):
            return self._process_pdf(api_url, model, api_key, input_data)
        else:
            return self._process_non_pdf(api_url, model, api_key, input_data)

    def _process_pdf(self, api_url, model, api_key, input_data):
        pdf_optimizer = PDFOptimizer()
        num_pages, output_files, temp_dir = pdf_optimizer.split_pdf_to_pages(
            input_data[0]["file_path"],
            convert_to_images=True
        )

        base64_images = []
        for page_file in output_files:
            with open(page_file, "rb") as f:
                base64_images.append(base64.b64encode(f.read()).decode("utf-8"))

        results = self._process_pages(api_url, model, api_key, base64_images, input_data)

        shutil.rmtree(temp_dir, ignore_errors=True)
        return results, num_pages

    def _process_non_pdf(self, api_url, model, api_key, input_data):
        file_path = input_data[0]["file_path"]

        with open(file_path, "rb") as f:
            base64_image = base64.b64encode(f.read()).decode("utf-8")

        results = self._process_pages(api_url, model, api_key, [base64_image], input_data)
        return results, 1

    def _process_pages(self, api_url, model, api_key, base64_images, input_data):
        results = []
        prompt = input_data[0].get("text_input", "")

        if len(base64_images) <= 4:
            # print(f"[+] Tek API isteği ile {len(base64_images)} sayfa/görüntü gönderiliyor.")
            results.append(self._call_api(api_url,model, api_key, base64_images, prompt))
        else:
            batch_size = 4
            for i in range(0, len(base64_images), batch_size):
                batch_images = base64_images[i:i + batch_size]
                page_range = f"{i + 1}-{i + len(batch_images)}"
                # print(f"[+] Sayfa/Görüntü {page_range} gönderiliyor ({len(batch_images)} sayfa/görüntü).")
                results.append(self._call_api(api_url, model, api_key, batch_images, prompt))

        return results

    def _call_api(self, api_url, model, api_key, base64_images, prompt):
        # print(prompt + "------\n")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

        content_block = [{"type": "text", "text": prompt}]
        for b64 in base64_images:
            content_block.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{b64}"
                }
            })

        data = {
            "model": f"{model}",
            "messages": [
                {
                    "role": "user",
                    "content": content_block
                }
            ],
            "temperature": 0.2
        }

        try:
            response = requests.post(api_url, headers=headers, json=data)
            response.raise_for_status()
            result_text = response.json()["choices"][0]["message"]["content"]
            print("[✓] API yanıtı alındı:")
            print(result_text)
            return result_text
        except Exception as e:
            print(f"[!] API hatası: {e}")
            return {"error": str(e)}