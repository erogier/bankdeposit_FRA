#!/usr/bin/env python3

import os, sys, re, json, time, argparse
import requests
import pandas as pd
from typing import List, Dict, Tuple, Optional

# =======================
# Config / Environment
# =======================
API_BASE = os.getenv("QARD_BASE", "https://api-demo.qardfinance.com")

# Option A (recommended): set via env var QARD_API_KEY
API_KEY  = os.getenv("QARD_API_KEY", "19ac7ba25eadf4c16d1ed56b98ef5da29691e388ff88426570dc1a954f6360ab")

UA       = "qard-bulk-doc-puller/1.0"

# Light politeness between network calls (seconds)
CALL_PAUSE = 0.20

# =======================
# HTTP helpers
# =======================
def session_json_headers():
    return {
        "accept": "application/json",
        "X-API-KEY": API_KEY,
        "User-Agent": UA,
    }

def session_bin_headers():
    return {
        "accept": "*/*",
        "X-API-KEY": API_KEY,
        "User-Agent": UA,
    }

def get_json(sess: requests.Session, path: str, params: dict = None, timeout: int = 20):
    url = f"{API_BASE.rstrip('/')}{path}"
    r = sess.get(url, headers=session_json_headers(), params=params or {}, timeout=timeout)
    try:
        data = r.json()
    except Exception:
        data = {"_text": r.text}
    return r.status_code, r.headers, data

# =======================
# Domain helpers
# =======================
def auth_check(sess: requests.Session) -> bool:
    code, _, me = get_json(sess, "/api/v6/clients")
    if code == 200:
        name = (me or {}).get("name", "unknown")
        print(f"✅ Auth OK. Client: {name}")
        return True
    print(f"❌ Auth check failed on /api/v6/clients: {code} {me}")
    return False

def safe_filename(name: str) -> str:
    return re.sub(r'[^\w.\-]+', '_', str(name))[:200]

def download_file(sess: requests.Session, file_id: str, outdir: str, prefix: str = "", overwrite: bool = False) -> str:
    url = f"{API_BASE.rstrip('/')}/api/v6/file/{file_id}"
    r = sess.get(url, headers=session_bin_headers(), stream=True, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Download failed: {r.status_code} {r.text[:200]}")

    # Name from Content-Disposition, fallback to file_id
    cd = r.headers.get("Content-Disposition", "")
    m = re.search(r'filename\*=UTF-8\'\'([^;]+)', cd) or re.search(r'filename="?([^";]+)"?', cd)
    fname = m.group(1) if m else f"{file_id}.pdf"

    if prefix:
        base, ext = os.path.splitext(fname)
        fname = f"{safe_filename(prefix)}__{safe_filename(base)}{ext or '.pdf'}"

    path = os.path.join(outdir, fname)
    os.makedirs(outdir, exist_ok=True)

    if os.path.exists(path) and not overwrite:
        return path  # skip if already there

    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    return path

def load_filelist_from_csv(csv_path: str) -> List[Dict]:
    """
    Loads a semi-colon separated CSV with at least `file_id`.
    Optional columns used for nicer naming: `siren`, `titles`, `date`.
    """
    if not os.path.exists(csv_path):
        print(f"❌ CSV not found: {csv_path}")
        return []
    # Many European CSVs are ';' delimited. Fall back to comma if needed.
    try:
        df = pd.read_csv(csv_path, sep=";", engine="python", encoding="utf-8", on_bad_lines="skip")
        if "file_id" not in df.columns and "fileid" in df.columns:
            df = df.rename(columns={"fileid": "file_id"})
    except Exception:
        df = pd.read_csv(csv_path)  # try default

    if "file_id" not in df.columns:
        print(f"❌ CSV must contain a 'file_id' column. Found columns: {list(df.columns)}")
        return []

    # Normalize and return as list of dicts
    df["file_id"] = df["file_id"].astype(str).str.strip()
    for col in ("siren", "titles", "date"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    records = df.to_dict(orient="records")
    return records

# =======================
# Main
# =======================
def main():
    ap = argparse.ArgumentParser(description="Qard API: download documents from a CSV of file IDs")
    ap.add_argument("--csv-file", default="acts_results.csv",
                    help="Path to the CSV containing file IDs (required column: file_id; optional: siren,titles,date)")
    ap.add_argument(
        "--outdir",
        default="/Users/emilerogier/Documents/PhD/Research projects/Advisory banks/data",
        help="Directory to save PDFs"
    )
    ap.add_argument("--overwrite", action="store_true", help="Redownload even if file already exists")
    args = ap.parse_args(args=[] if hasattr(sys, 'ps1') or 'PYCHARM_HOSTED' in os.environ else None)

    if not API_KEY:
        print("❌ Missing API key. Either set QARD_API_KEY env var, or hardcode API_KEY in the script.")
        sys.exit(2)

    # Session + auth (still needed to hit /api/v6/file/{id})
    sess = requests.Session()
    if not auth_check(sess):
        sys.exit(1)

    rows = load_filelist_from_csv(args.csv_file)
    if not rows:
        sys.exit(1)

    total_rows = len(rows)
    total_attempted = 0
    total_downloaded = 0
    errors = []

    print(f"📄 Loaded {total_rows} rows from CSV: {args.csv_file}")
    print(f"📥 Output directory: {args.outdir}")

    for idx, row in enumerate(rows, 1):
        file_id = (row.get("file_id") or "").strip()
        if not file_id:
            print(f"[{idx}/{total_rows}] ⚠️ Missing file_id, skipping")
            continue

        # Build a nice prefix for filenames: SIREN_ACT_YYYYMMDD__TITLE (when available)
        siren = row.get("siren") or ""
        title = row.get("titles") or ""
        date = (row.get("date") or "").replace("/", "-").replace(" ", "_")
        parts = []
        if siren:
            parts.append(siren)
        parts.append("ACT")
        if date:
            parts.append(date)
        if title:
            parts.append(title)
        prefix = "__".join([safe_filename(p) for p in parts if p])

        try:
            print(f"[{idx}/{total_rows}] ⬇️  Downloading file_id={file_id} …")
            path = download_file(sess, file_id, args.outdir, prefix=prefix, overwrite=args.overwrite)
            print(f"   ✅ Saved: {path}")
            total_downloaded += 1
        except Exception as e:
            print(f"   ❌ Download failed for file_id={file_id}: {e}")
            errors.append((file_id, str(e)))
        finally:
            total_attempted += 1
            time.sleep(CALL_PAUSE)

    # Summary
    print("\n================ SUMMARY ================")
    print(f"Total rows in CSV:         {total_rows}")
    print(f"Total download attempts:   {total_attempted}")
    print(f"Total files downloaded:    {total_downloaded}")
    if errors:
        print(f"Errors ({len(errors)}):")
        for fid, msg in errors[:10]:
            print(f"  - {fid}: {msg}")
        if len(errors) > 10:
            print("  ...")

if __name__ == "__main__":
    main()
