#!/usr/bin/env python3
import os, sys, re, json, time, argparse
import requests
from typing import List, Dict, Tuple, Optional

# =======================
# Config / Environment
# =======================
API_BASE = os.getenv("QARD_BASE", "https://api-demo.qardfinance.com")

# Option A (recommended): set via env var QARD_API_KEY
API_KEY  = os.getenv("QARD_API_KEY", "19ac7ba25eadf4c16d1ed56b98ef5da29691e388ff88426570dc1a954f6360ab")

# Option B (run from PyCharm without env vars): uncomment and paste your key
# API_KEY = "PUT_YOUR_QARD_API_KEY_HERE"

UA       = "qard-bulk-doc-puller/1.0"

# Allowed datatypes to download (uppercase). Adjust as needed.
ALLOWED_DATATYPES = {
    "ACT",
    "ARTICLES_OF_ASSOCIATION",
    "LEGAL_NOTICE",
    "AVIS_SIREN",
}

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

def iter_pages(sess: requests.Session, path: str, base_params: dict, item_key_candidates: List[str]):
    """
    Generic paginator for endpoints that return:
      { total, per_page, current_page, last_page, result: [...] }
    or a list directly (then we yield once).
    """
    page = 1
    while True:
        params = dict(base_params or {})
        params.setdefault("per_page", 100)
        params["page"] = page
        code, headers, data = get_json(sess, path, params=params)
        if code != 200:
            yield code, headers, data
            return
        # list directly
        if isinstance(data, list):
            for it in data:
                yield 200, headers, it
            return
        # object with a result-like array
        items = None
        for k in item_key_candidates:
            if isinstance(data.get(k), list):
                items = data[k]
                break
        if items is None:
            # nothing we can iterate
            yield 200, headers, data
            return
        for it in items:
            yield 200, headers, it
        # pagination footer
        cur = int(data.get("current_page", page))
        last = int(data.get("last_page", cur))
        if cur >= last:
            return
        page += 1
        time.sleep(CALL_PAUSE)

# =======================
# Domain helpers
# =======================
def load_sirens(path: str) -> List[str]:
    out = []
    if not os.path.exists(path):
        print(f"❌ SIREN list file not found: {path}")
        return out
    with open(path, "r") as f:
        for line in f:
            s = line.strip().replace(" ", "")
            if len(s) == 9 and s.isdigit():
                out.append(s)
    return out

def auth_check(sess: requests.Session) -> bool:
    code, _, me = get_json(sess, "/api/v6/clients")
    if code == 200:
        name = (me or {}).get("name", "unknown")
        print(f"✅ Auth OK. Client: {name}")
        return True
    print(f"❌ Auth check failed on /api/v6/clients: {code} {me}")
    return False

def find_user_by_siren(sess: requests.Session, siren: str) -> Optional[Dict]:
    """
    Tries:
      - /api/v6/users with pagination (filter locally)
      - /api/v6/users?siren=... (and other common keys)
    Returns the first exact match (stripped equality).
    """
    # A) Paginate and filter locally (works on demo)
    for code, _, item in iter_pages(sess, "/api/v6/users", {"per_page": 100}, ["result"]):
        if code != 200:
            break
        if not isinstance(item, dict):
            continue
        if str(item.get("siren", "")).strip() == siren:
            return item

    # B) Common server-side search params
    for key in ("siren", "query", "search", "q"):
        code, _, data = get_json(sess, "/api/v6/users", params={key: siren, "per_page": 50})
        if code == 200 and isinstance(data, dict):
            arr = data.get("result") if isinstance(data.get("result"), list) else (data.get("users") if isinstance(data.get("users"), list) else None)
            if arr:
                for u in arr:
                    if str(u.get("siren", "")).strip() == siren:
                        return u
    return None

def list_files_for_user(sess: requests.Session, user_id: str, siren: Optional[str]) -> List[Dict]:
    """
    Tries the common shapes:
      - /api/v6/users/{id}/files
      - /api/v6/files?user_id=...
      - /api/v6/files?siren=...
    Returns a flat list of file dicts.
    """
    # A) Nested route
    code, _, data = get_json(sess, f"/api/v6/users/{user_id}/files", params={"per_page": 100})
    if code == 200:
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in ("result", "files"):
                if isinstance(data.get(k), list):
                    return data[k]

    # B) Flat listing by user_id
    code, _, data = get_json(sess, "/api/v6/files", params={"user_id": user_id, "per_page": 100})
    if code == 200:
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in ("result", "files"):
                if isinstance(data.get(k), list):
                    return data[k]

    # C) Flat listing by siren
    if siren:
        code, _, data = get_json(sess, "/api/v6/files", params={"siren": siren, "per_page": 100})
        if code == 200:
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                for k in ("result", "files"):
                    if isinstance(data.get(k), list):
                        return data[k]
    return []

def pick_files(files: List[Dict], download_all: bool, max_files_per_user: int) -> List[Tuple[str, str]]:
    """
    Returns a list of (file_id, datatype) tuples to download.
    If download_all=False, returns at most one file.
    """
    chosen = []
    for f in files:
        dtype = (f.get("datatype") or f.get("data_type") or "").upper()
        if ALLOWED_DATATYPES and dtype and dtype not in ALLOWED_DATATYPES:
            continue
        fid = f.get("id") or f.get("file_id") or f.get("uuid")
        if not fid:
            continue
        chosen.append((fid, dtype or "UNKNOWN"))
        if not download_all and chosen:
            break
        if download_all and len(chosen) >= max_files_per_user:
            break
    return chosen

def safe_filename(name: str) -> str:
    return re.sub(r'[^\w.\-]+', '_', name)[:200]

def download_file(sess: requests.Session, file_id: str, outdir: str, prefix: str = "") -> str:
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
    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    return path

# =======================
# Main
# =======================
# =======================
# Main
# =======================
def main():
    # Make --sirens-file OPTIONAL, defaulting to the existing file
    ap = argparse.ArgumentParser(description="Qard API: bulk list & download documents for SIRENs")
    ap.add_argument("--sirens-file", default="sirens_existing_diffusible.txt",
                    help="Path to a text file with one 9-digit SIREN per line (default: sirens_existing_diffusible.txt)")
    # ✅ Updated default outdir to your dedicated folder
    ap.add_argument(
        "--outdir",
        default="/Users/emilerogier/Documents/PhD/Research projects/Advisory banks/data",
        help="Directory to save PDFs"
    )
    ap.add_argument("--all-files", action="store_true", help="Download all allowed files per SIREN (default: only first)")
    ap.add_argument("--max-files-per-user", type=int, default=10, help="Cap when --all-files is used")
    args = ap.parse_args(args=[] if hasattr(sys, 'ps1') or 'PYCHARM_HOSTED' in os.environ else None)
    # The ^^ makes running from PyCharm/REPL work without typing CLI args.

    if not API_KEY:
        print("❌ Missing API key. Either set QARD_API_KEY env var, or hardcode API_KEY in the script.")
        sys.exit(2)

    sirens = load_sirens(args.sirens_file)
    if not sirens:
        print(f"❌ No valid SIRENs found in file: {args.sirens_file}")
        sys.exit(1)

    sess = requests.Session()

    if not auth_check(sess):
        sys.exit(1)

    total_found = 0
    total_downloaded = 0
    missing_users = []
    no_files = []
    errors = []

    for idx, siren in enumerate(sirens, 1):
        print(f"\n[{idx}/{len(sirens)}] SIREN {siren}")

        # 1) Find user
        user = find_user_by_siren(sess, siren)
        if not user:
            print("   ❌ No user found with that SIREN.")
            missing_users.append(siren)
            time.sleep(CALL_PAUSE)
            continue
        user_id = user.get("id")
        user_name = user.get("name") or user.get("display_name") or "unknown"
        print(f"   ✅ User: {user_name} (id={user_id})")

        # 2) List files
        files = list_files_for_user(sess, user_id, siren)
        print(f"   📄 Files available: {len(files)}")
        if not files:
            no_files.append(siren)
            time.sleep(CALL_PAUSE)
            continue

        # 3) Choose and download
        chosen = pick_files(files, args.all_files, args.max_files_per_user)
        if not chosen:
            print("   ⚠️ No files matching allowed datatypes.")
            no_files.append(siren)
            time.sleep(CALL_PAUSE)
            continue

        total_found += len(chosen)
        for n, (file_id, dtype) in enumerate(chosen, 1):
            try:
                prefix = f"{siren}_{dtype}_{n:02d}" if args.all_files else f"{siren}_{dtype}"
                print(f"   ⬇️  Downloading {file_id} (datatype={dtype}) …")
                path = download_file(sess, file_id, args.outdir, prefix=prefix)
                print(f"   ✅  Saved: {path}")
                total_downloaded += 1
            except Exception as e:
                print(f"   ❌ Download failed for file {file_id}: {e}")
                errors.append((siren, file_id, str(e)))
            time.sleep(CALL_PAUSE)

        time.sleep(CALL_PAUSE)

    # Summary
    print("\n================ SUMMARY ================")
    print(f"Total SIRENs processed:   {len(sirens)}")
    print(f"Total files selected:     {total_found}")
    print(f"Total files downloaded:   {total_downloaded}")
    if missing_users:
        print(f"Users not found ({len(missing_users)}): {', '.join(missing_users[:20])}{' ...' if len(missing_users) > 20 else ''}")
    if no_files:
        print(f"No files ({len(no_files)}): {', '.join(no_files[:20])}{' ...' if len(no_files) > 20 else ''}")
    if errors:
        print(f"Errors ({len(errors)}):")
        for s, fid, msg in errors[:10]:
            print(f"  - {s} / {fid}: {msg}")
        if len(errors) > 10:
            print("  ...")

if __name__ == "__main__":
    main()