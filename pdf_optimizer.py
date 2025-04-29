import os
import tempfile
import shutil
import pdf2image
from PIL import Image
import logging

class PDFOptimizer:
    """PDF dosyalarını işlemek ve optimize etmek için yardımcı sınıf."""
    
    def split_pdf_to_pages(self, pdf_path, debug_dir=None, convert_to_images=True):
        """
        PDF dosyasını sayfalara böler ve isteğe bağlı olarak görüntülere dönüştürür.
        
        Args:
            pdf_path (str): PDF dosyasının yolu.
            debug_dir (str): Hata ayıklama çıktıları için klasör.
            convert_to_images (bool): Sayfaları görüntülere dönüştürme bayrağı.
            
        Returns:
            tuple: (sayfa_sayısı, sayfa_dosyaları, geçici_dizin)
        """
        try:
            # Geçici klasör oluştur
            temp_dir = tempfile.mkdtemp()
            
            if convert_to_images:
                # PDF'yi görüntülere dönüştür
                images = pdf2image.convert_from_path(
                    pdf_path, 
                    dpi=300,
                    output_folder=temp_dir if not debug_dir else debug_dir,
                    fmt="png"
                )
                
                # Dosya yollarını hazırla
                output_files = []
                for i, img in enumerate(images):
                    page_path = os.path.join(temp_dir if not debug_dir else debug_dir, f"page_{i+1}.png")
                    if not os.path.exists(page_path):  # Eğer dosya henüz kaydedilmemişse
                        img.save(page_path, "PNG")
                    output_files.append(page_path)
                
                return len(images), output_files, temp_dir
            else:
                # TODO: PDF sayfalarını ayrı PDF dosyaları olarak ayırma işlevi
                # Bu özelliğe ihtiyacınız olursa ekleyin
                pass
                
        except Exception as e:
            logging.error(f"PDF işleme hatası: {e}")
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
            raise
            
        return 0, [], temp_dir