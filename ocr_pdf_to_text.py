
import os, shutil
from pdf2image import convert_from_path
from PIL import Image, ImageOps
import pytesseract

PDF_PATH = "821224268_63df27df8640ab3541151a61.pdf"          # <-- set this
OUT_TXT  = "821224268_63df27df8640ab3541151a61.pdf.txt"  # output path

# 1) (Optional) allow very large images if you trust the PDF
Image.MAX_IMAGE_PIXELS = None  # comment this out if you prefer the safety check

# 2) Tesseract binary (PyCharm-safe)
pytesseract.pytesseract.tesseract_cmd = shutil.which("tesseract") or "/opt/homebrew/bin/tesseract"

# 3) Poppler path for pdf2image (PyCharm may not inherit PATH)
brew_bins = ["/opt/homebrew/bin", "/usr/local/bin"]
poppler_path = next((p for p in brew_bins if os.path.exists(os.path.join(p, "pdftoppm"))), None)

# 4) Convert PDF pages to images
#    - dpi=300 is usually enough
#    - size=3508 limits the longest side ~ A4 at 300 dpi (≈ 8.7 MP), avoids bomb errors
pages = convert_from_path(
    PDF_PATH,
    dpi=300,
    size=3508,                # cap the longest side (or use a tuple like (2480, 3508))
    fmt="png",
    poppler_path=poppler_path,
    thread_count=4
)

texts = []
with open(OUT_TXT, "w", encoding="utf-8") as f:
    for i, img in enumerate(pages, 1):
        gray = ImageOps.grayscale(img)
        # For multi-column pages, psm 4 can work better; try 6/3/1 if needed
        txt = pytesseract.image_to_string(gray, lang="fra", config="--oem 3 --psm 4")
        f.write(f"===== Page {i} =====\n{txt}\n")

print(f"Saved OCR text to {OUT_TXT}")
