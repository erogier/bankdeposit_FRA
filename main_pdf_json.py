"""
INPI legal documents downloader → JSON output version
----------------------------------------------------
Goal:
  - Log in to INPI, fetch a company's documents (actes), filter to allowed types,
  - Download each matching PDF, and produce a single JSON file that contains,
    for every document: SIREN, typeACTE, INPI document id, and the document itself
    encoded as Base64 (so it fits inside JSON).

Security reminder:
  • Prefer environment variables for credentials (see notes below).

Output:
  • A file named:  inpi_actes_<SIREN>.json
  • Each item in the JSON array has keys:
        {
          "siren": "500569405",
          "typeActe": "statuts constitutifs",
          "id": "123456",
          "document_base64": "...",
          "mime": "application/pdf"
        }

Dependencies:
  • Python 3
  • requests

Run:
  python3 download_inpi_json.py
"""

import os
import re
import json
import base64
import requests
import unicodedata
from typing import List, Dict, Any

# ---------------------------
# 1) Configuration (edit me)
# ---------------------------
# Safer alternative (recommended):
# username = os.getenv("INPI_USERNAME")
# password = os.getenv("INPI_PASSWORD")
username = "emile.rogier@hec.edu"
password = "Hippolyte=12"

# SIREN is the 9‑digit company identifier in France (no spaces)
siren = "834816985"

# INPI API endpoints used by this script
login_url = "https://registre-national-entreprises.inpi.fr/api/sso/login"
attachments_url = f"https://registre-national-entreprises.inpi.fr/api/companies/{siren}/attachments"

# We keep the login token in a small text file so we don't have to log in every time
# (until it expires). This is placed next to the script.
token_file = "inpi_token.txt"

# ------------------------------------------
# 2) Which document types should we include?
# ------------------------------------------
allowed_types = [
    "statuts constitutifs",
    "attestation de depot des fonds",
    "attestation bancaire",
]

# -----------------------------
# 3) Helpers: normalize/labels
# -----------------------------

def normalize(text: str) -> str:
    """Remove accents, non-ASCII, lower-case."""
    return unicodedata.normalize('NFKD', text or "").encode('ascii', 'ignore').decode().lower()


def choose_acte_label(acte: Dict[str, Any]) -> str:
    """Pick a representative label for the acte, preferring allowed types if present."""
    type_rdd_list = acte.get("typeRdd", [])
    labels_raw = [t.get("typeActe", "") for t in type_rdd_list]
    labels_norm = [normalize(lbl) for lbl in labels_raw]

    for raw, norm in zip(labels_raw, labels_norm):
        if any(allowed in norm for allowed in allowed_types):
            return raw  # return the human-readable original if it matched

    # Fallback: first available original label, else "inconnu"
    return labels_raw[0] if labels_raw else "inconnu"

# --------------------------------------------------------
# 4) Token storage helpers: save/load/clear the login token
# --------------------------------------------------------

def save_token(token: str) -> None:
    with open(token_file, "w") as f:
        f.write(token)

def load_token() -> str | None:
    if os.path.exists(token_file):
        with open(token_file, "r") as f:
            return f.read().strip()
    return None

def clear_token() -> None:
    if os.path.exists(token_file):
        os.remove(token_file)

# ----------------------
# 5) Login + token reuse
# ----------------------

def login() -> str | None:
    payload = {"username": username, "password": password}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(login_url, json=payload, headers=headers, timeout=15)
        response.raise_for_status()
        token = response.json().get("token")
        if not token:
            print("❌ Login failed: No token received")
            return None
        save_token(token)
        print("✅ Login successful. Token saved.")
        return token
    except requests.RequestException as e:
        print(f"❌ Login failed: {e}")
        return None


def get_token() -> str | None:
    token = load_token()
    if token:
        print("ℹ️ Using saved token.")
        return token
    return login()

# ------------------------------------
# 6) Fetch attachments metadata (JSON)
# ------------------------------------

def fetch_attachments(token: str) -> Dict[str, Any] | None:
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(attachments_url, headers=headers, timeout=20)
        if response.status_code == 401:
            print("⚠️ Token expired or unauthorized, retrying login...")
            clear_token()
            new_token = login()
            if not new_token:
                return None
            return fetch_attachments(new_token)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"❌ Failed to get attachments: {e}")
        return None

# --------------------------------------
# 7) Filter to allowed types (if desired)
# --------------------------------------

def filter_actes(attachments_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    actes = attachments_json.get("actes", [])
    filtered = []
    for acte in actes:
        type_rdd_list = acte.get("typeRdd", [])
        acte_types_norm = [normalize(t.get("typeActe", "")) for t in type_rdd_list]
        if any(any(allowed in t for allowed in allowed_types) for t in acte_types_norm):
            filtered.append(acte)
    return filtered

# -----------------------------------------------
# 8) Download a single acte and return base64 data
# -----------------------------------------------

def download_acte_base64(token: str, acte: Dict[str, Any]) -> str | None:
    acte_id = acte.get("id")
    url = f"https://registre-national-entreprises.inpi.fr/api/actes/{acte_id}/download"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 401:
            print("⚠️ Token expired during download, retrying login...")
            clear_token()
            new_token = login()
            if not new_token:
                print("❌ Cannot download, login failed.")
                return None
            return download_acte_base64(new_token, acte)
        resp.raise_for_status()
        b64 = base64.b64encode(resp.content).decode("ascii")
        return b64
    except requests.RequestException as e:
        print(f"❌ Failed to download {acte_id}: {e}")
        return None

# ------------------
# 9) Main: build JSON
# ------------------

def main() -> None:
    token = get_token()
    if not token:
        print("❌ Cannot proceed without token.")
        return

    attachments = fetch_attachments(token)
    if not attachments:
        print("❌ No attachments found or failed to fetch.")
        return

    actes = filter_actes(attachments)
    print(f"📄 Found {len(actes)} acte(s) matching criteria.")

    output: List[Dict[str, Any]] = []
    for acte in actes:
        label = choose_acte_label(acte)
        acte_id = acte.get("id")
        b64 = download_acte_base64(token, acte)
        if not b64:
            continue
        output.append({
            "siren": siren,
            "typeActe": label,
            "id": acte_id,
            "mime": "application/pdf",
        })

    out_path = f"inpi_actes_{siren}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"✅ Wrote {len(output)} item(s) to {out_path}")


if __name__ == "__main__":
    main()
