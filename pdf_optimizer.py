import os
import tempfile
import shutil
import pdf2image
import logging


class PDFOptimizer:

    def split_pdf_to_pages(self, pdf_path, convert_to_images=True):
        try:
            temp_dir = tempfile.mkdtemp()

            if convert_to_images:
                images = pdf2image.convert_from_path(
                    pdf_path,
                    dpi=300,
                    output_folder=temp_dir,
                    fmt="png"
                )

                output_files = []
                for i, img in enumerate(images):
                    page_path = os.path.join(temp_dir, f"page_{i + 1}.png")
                    if not os.path.exists(page_path):
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