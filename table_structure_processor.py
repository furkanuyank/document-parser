import os
import cv2
import numpy as np
from PIL import Image
import logging
try:
    from rich.progress import Progress, SpinnerColumn, TextColumn
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

class TableDetector:
    """Görüntülerdeki tabloları tespit eden ve işleyen sınıf."""

    def detect_tables(self, file_path, local=True, debug_dir=None, debug=False):
        """
        Bir görüntüdeki tabloları tespit eder ve kırpar.
        
        Args:
            file_path: Görüntü dosyasının yolu.
            local: Yerel çalışma modu.
            debug_dir: Hata ayıklama çıktıları için klasör.
            debug: Hata ayıklama bayrağı.
            
        Returns:
            list: Kırpılmış tablo görüntüleri listesi.
        """
        try:
            # Görüntüyü yükle
            image = cv2.imread(file_path)
            if image is None:
                if debug:
                    print(f"Görüntü yüklenemedi: {file_path}")
                return []
                
            # Görüntüyü gri tonlamaya dönüştür
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            
            # Kenarları algıla
            task_description = "Tablo kenarları algılanıyor..."
            def task_call():
                edges = cv2.Canny(gray, 100, 200)
                return edges
            edges = self.invoke_pipeline_step(task_call, task_description, local)
            
            # Çizgileri algıla (Hough dönüşümü)
            task_description = "Tablo çizgileri algılanıyor..."
            def task_call():
                lines = cv2.HoughLinesP(edges, 1, np.pi/180, threshold=100, minLineLength=100, maxLineGap=10)
                return lines
            lines = self.invoke_pipeline_step(task_call, task_description, local)
            
            if lines is None:
                if debug:
                    print("Çizgi bulunamadı.")
                return []
                
            # Görüntü boyutlarını al
            height, width = gray.shape
            
            # Yatay ve dikey çizgileri ayır
            horizontal_lines = []
            vertical_lines = []
            for line in lines:
                x1, y1, x2, y2 = line[0]
                if abs(x2 - x1) > abs(y2 - y1):  # Yatay çizgi
                    horizontal_lines.append((x1, y1, x2, y2))
                else:  # Dikey çizgi
                    vertical_lines.append((x1, y1, x2, y2))
                    
            # Tablo sınırlarını belirle (basit yaklaşım)
            def find_table_boundaries():
                if not horizontal_lines or not vertical_lines:
                    return None
                    
                # En soldaki ve en sağdaki dikey çizgiler
                min_x = width
                max_x = 0
                for x1, y1, x2, y2 in vertical_lines:
                    min_x = min(min_x, min(x1, x2))
                    max_x = max(max_x, max(x1, x2))
                    
                # En üstteki ve en alttaki yatay çizgiler
                min_y = height
                max_y = 0
                for x1, y1, x2, y2 in horizontal_lines:
                    min_y = min(min_y, min(y1, y2))
                    max_y = max(max_y, max(y1, y2))
                    
                # Çok küçük alanları filtreleme
                if max_x - min_x < width * 0.1 or max_y - min_y < height * 0.1:
                    return None
                    
                return [min_x, min_y, max_x, max_y]
                
            task_description = "Tablo sınırları belirleniyor..."
            table_boundaries = self.invoke_pipeline_step(find_table_boundaries, task_description, local)
            
            if not table_boundaries:
                if debug:
                    print("Tablo sınırları belirlenemedi.")
                return []
                
            # Tabloyu kırp
            min_x, min_y, max_x, max_y = table_boundaries
            # Sınırları genişlet
            padding = 10
            min_x = max(0, min_x - padding)
            min_y = max(0, min_y - padding)
            max_x = min(width, max_x + padding)
            max_y = min(height, max_y + padding)
            
            table_image = image[min_y:max_y, min_x:max_x]
            
            # Kırpılmış görüntüyü PIL formatına dönüştür ve kaydet
            table_image_pil = Image.fromarray(cv2.cvtColor(table_image, cv2.COLOR_BGR2RGB))
            
            # Hata ayıklama için görüntüleri kaydet
            if debug and debug_dir:
                task_description = "Hata ayıklama görüntüleri kaydediliyor..."
                def save_debug_images():
                    # Orijinal görüntü
                    original = Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
                    original.save(os.path.join(debug_dir, "original.png"))
                    
                    # Kenarlar
                    edges_img = Image.fromarray(edges)
                    edges_img.save(os.path.join(debug_dir, "edges.png"))
                    
                    # Tablo
                    table_image_pil.save(os.path.join(debug_dir, "table.png"))
                    
                self.invoke_pipeline_step(save_debug_images, task_description, local)
                
            return [table_image_pil]
            
        except Exception as e:
            logging.error(f"Tablo algılama hatası: {e}")
            return []

    @staticmethod
    def invoke_pipeline_step(task_call, task_description, local):
        """
        Boru hattı adımını çağırır ve görsel ilerleme gösterir.
        
        Args:
            task_call: Çağrılacak görev fonksiyonu.
            task_description: Görev açıklaması.
            local: Yerel çalışma modu.
            
        Returns:
            Görev çağrısının sonucu.
        """
        if local and HAS_RICH:
            with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    transient=False,
            ) as progress:
                progress.add_task(description=task_description, total=None)
                ret = task_call()
        else:
            print(task_description)
            ret = task_call()

        return ret