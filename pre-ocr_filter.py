#!/usr/bin/env python3
"""
Filter a multi‑page PDF and keep only pages likely to be French bank
"certificats de dépôt" (certificates of deposit) BEFORE doing full OCR.

How it works
------------
1) For each page, try fast text extraction with pypdf.
2) If no (or tiny) text is found, do a QUICK low‑DPI OCR of that page only.
3) Score the page using French keywords/phrases (accent‑insensitive).
4) Write two PDFs:
   - *_CD_pages.pdf       → pages likely about certificats de dépôt
   - *_nonCD_pages.pdf    → everything else
5) Also write a CSV audit file with per‑page scores and snippets so you can tweak.

Dependencies (install in your PyCharm interpreter)
-------------------------------------------------
pip install pypdf pdf2image pillow pytesseract
# system deps on macOS (Terminal):
#   brew install poppler tesseract tesseract-lang

Edit the PDF_IN path below, then run this file in PyCharm.
"""

import csv
import io
import os
import re
import shutil
import sys
import unicodedata
from typing import Tuple, List

from pypdf import PdfReader, PdfWriter
from pdf2image import convert_from_path
from PIL import Image, ImageOps
import pytesseract

# ----------------------- USER SETTINGS ----------------------- #
PDF_IN = "823581467_63e073178eded29cb31d96d8.pdf"  # ← set this to your input PDF
PRE_OCR_DPI = 150          # quick skim DPI for image‑only pages (150–200 is fine)
MAX_LONG_SIDE = 2500       # cap the longest side during quick OCR to keep memory sane
LANGS = "fra"              # or "fra+eng" if mixed

# Keywords (accent‑insensitive; accents removed during matching)
CORE_PHRASES = [
    "certificat de depot",
    "certificat de depot negociable",
    "attestation de depot de fonds",
    "attestation de depot",
    "attestation bancaire",
    "attestation de blocage du capital",
    "attestation de blocage",
    "depot de capital",
    "depot du capital",
]
KEYWORDS = [
    # banking + identifiers
    "banque", "etablissement", "agence", "guichet", "adresse", "cedex",
    "compte special", "numero de compte", "compte n", "iban", "bic", "swift", "rib",
    # amounts, roles, process
    "somme", "montant verse", "versement", "deposant", "mandataire", "signature",
    # registration & corporate refs that appear on attestations
    "rcs", "registre du commerce", "kbis", "greffe",
    # timing & blocking
    "date de valeur", "date d'emission", "certificat d'immatriculation", "blocage", "bloquee",
    # optional finance words for negotiable CDs
    "taux", "rendement", "echeance", "maturite", "nominal", "emission",
]
NEGATIVE_KEYWORDS = [
    # pages likely to be company statutes or legal code text
    "statuts", "extrait des statuts", "statut",
    "article", "art.", "chapitre", "titre", "section", "clause",
]

# Decision thresholds
CORE_HIT_POINTS = 3
KEYWORD_POINTS = 1
NEGATIVE_POINTS = -1
KEEP_THRESHOLD = 4  # keep page if score >= 4 OR (has core phrase AND any other keyword)

# ------------------------------------------------------------ #

# Safety: allow large images for big pages during quick OCR (you can disable if you prefer)
Image.MAX_IMAGE_PIXELS = None


def norm(text: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    if not text:
        return ""
    t = unicodedata.normalize("NFKD", text)
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = t.lower()
    t = re.sub(r"\s+", " ", t).strip()
    return t


def find_poppler_bin() -> str:
    """Return path to poppler bin dir if needed for pdf2image on macOS."""
    candidates = ["/opt/homebrew/bin", "/usr/local/bin"]
    for p in candidates:
        if os.path.exists(os.path.join(p, "pdftoppm")):
            return p
    return None


def find_tesseract_bin() -> str:
    return shutil.which("tesseract") or "/opt/homebrew/bin/tesseract"


pytesseract.pytesseract.tesseract_cmd = find_tesseract_bin()
POPPLER_PATH = find_poppler_bin()


def try_extract_text(reader: PdfReader, page_index: int) -> Tuple[str, bool]:
    """
    Try pypdf text extraction; if empty/short, do a quick per‑page OCR.
    Returns (text, used_ocr?).
    """
    page = reader.pages[page_index]
    txt = page.extract_text() or ""
    if len(txt.strip()) >= 80:
        return txt, False

    # quick OCR for this page only
    images = convert_from_path(
        PDF_IN,
        dpi=PRE_OCR_DPI,
        first_page=page_index + 1,
        last_page=page_index + 1,
        fmt="png",
        size=MAX_LONG_SIDE,
        poppler_path=POPPLER_PATH,
    )
    img = images[0]
    gray = ImageOps.grayscale(img)
    # psm 4: assume column detection; tweak to 6/3/1 if needed
    ocr_txt = pytesseract.image_to_string(gray, lang=LANGS, config="--oem 3 --psm 4")
    return ocr_txt, True


def score_page(text: str) -> Tuple[int, int, int, bool]:
    """Return (score, core_hits, kw_hits, has_core)."""
    t = norm(text)
    score = 0
    core_hits = sum(1 for p in CORE_PHRASES if p in t)
    kw_hits = sum(1 for k in KEYWORDS if k in t)
    neg_hits = sum(1 for n in NEGATIVE_KEYWORDS if n in t)

    score += core_hits * CORE_HIT_POINTS
    score += kw_hits * KEYWORD_POINTS
    score += neg_hits * NEGATIVE_POINTS

    has_core = core_hits > 0
    return score, core_hits, kw_hits, has_core


def should_keep(score: int, has_core: bool, kw_hits: int) -> bool:
    return (score >= KEEP_THRESHOLD) or (has_core and kw_hits >= 1)


def main():
    if not os.path.exists(PDF_IN):
        print(f"Input PDF not found: {PDF_IN}")
        sys.exit(1)

    base, _ = os.path.splitext(PDF_IN)
    out_keep = f"{base}_CD_pages.pdf"
    out_other = f"{base}_nonCD_pages.pdf"
    audit_csv = f"{base}_page_audit.csv"

    reader = PdfReader(PDF_IN)
    n = len(reader.pages)

    keep_writer = PdfWriter()
    other_writer = PdfWriter()

    rows: List[List[str]] = []

    print(f"Scanning {n} pages from {PDF_IN}…")

    for i in range(n):
        text, used_ocr = try_extract_text(reader, i)
        score, core_hits, kw_hits, has_core = score_page(text)
        keep = should_keep(score, has_core, kw_hits)

        # short snippet for audit (normalized for readability)
        snippet = norm(text)[:180]

        rows.append([
            str(i + 1),
            str(score),
            str(core_hits),
            str(kw_hits),
            "ocr" if used_ocr else "pypdf",
            "keep" if keep else "other",
            snippet,
        ])

        (keep_writer if keep else other_writer).add_page(reader.pages[i])
        print(f"Page {i+1:>3}: score={score:>2} core={core_hits} kw={kw_hits} via={'OCR' if used_ocr else 'pypdf'} → {'KEEP' if keep else 'other'}")

    # Write outputs
    if keep_writer.get_num_pages() > 0:
        with open(out_keep, "wb") as f:
            keep_writer.write(f)
        print(f"→ Wrote likely CD pages to: {out_keep}")
    else:
        print("→ No pages matched the CD criteria (try lowering threshold or adding keywords).")

    if other_writer.get_num_pages() > 0:
        with open(out_other, "wb") as f:
            other_writer.write(f)
        print(f"→ Wrote non‑CD pages to: {out_other}")

    # Audit CSV
    with open(audit_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["page", "score", "core_hits", "kw_hits", "method", "decision", "snippet"])
        w.writerows(rows)
    print(f"→ Wrote audit CSV: {audit_csv}")

    print("\nNow you can run your full OCR on the *_CD_pages.pdf only (faster & focused).\n")


if __name__ == "__main__":
    main()
