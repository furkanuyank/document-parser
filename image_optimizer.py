import os
import cv2
import numpy as np
from PIL import Image
import logging

class ImageOptimizer:
    """Görüntüleri işlemek ve optimize etmek için yardımcı sınıf."""
    
    def crop_image_borders(self, image_path, output_dir, debug_dir=None, crop_size=10):
        """
        Görüntünün kenarlarını kırpar.
        
        Args:
            image_path (str): Görüntü dosyasının yolu.
            output_dir (str): Çıktı klasörü.
            debug_dir (str): Hata ayıklama çıktıları için klasör.
            crop_size (int): Kırpılacak kenarlık boyutu (piksel).
            
        Returns:
            str: Kırpılmış görüntünün dosya yolu.
        """
        try:
            # Görüntüyü yükle
            img = Image.open(image_path)
            width, height = img.size
            
            # Kırpma bölgesini hesapla
            left = crop_size
            top = crop_size
            right = width - crop_size
            bottom = height - crop_size
            
            # Görüntüyü kırp
            cropped_img = img.crop((left, top, right, bottom))
            
            # Dosya yolu oluştur ve kaydet
            file_name = os.path.basename(image_path)
            name, ext = os.path.splitext(file_name)
            cropped_path = os.path.join(output_dir, f"{name}_cropped{ext}")
            cropped_img.save(cropped_path)
            
            # Hata ayıklama klasörüne de kaydet
            if debug_dir:
                debug_path = os.path.join(debug_dir, f"{name}_cropped{ext}")
                cropped_img.save(debug_path)
                
            return cropped_path
            
        except Exception as e:
            logging.error(f"Görüntü işleme hatası: {e}")
            return image_path  # Hata durumunda orijinal görüntüyü döndür