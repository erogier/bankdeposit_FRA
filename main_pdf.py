"""
INPI legal documents downloader (friendly commented version)
-----------------------------------------------------------------
Goal:
  - Log in to the INPI website (official business registry) with a username/password
  - Ask for the list of documents ("attachments") for a given company (identified by its SIREN)
  - Keep only the types of documents we care about
  - Download those matching documents to PDF files on your computer

Read me first (important!):
  • Never share or commit your real username/password in a public place (email, GitHub, etc.).
  • Ideally store secrets in environment variables instead of writing them in the file.
    Example (Terminal):
        export INPI_USERNAME="your.email@example.com"
        export INPI_PASSWORD="your-strong-password"
    Then in Python you can read them with os.getenv("INPI_USERNAME").

What you need installed:
  • Python 3
  • The "requests" library:  pip install requests

How to run:
  • Save this file (e.g., as download_inpi.py)
  • In Terminal:  python3 download_inpi.py
"""

import os
import requests
import unicodedata
import time

# ---------------------------
# 1) Configuration (edit me)
# ---------------------------
# ⚠️ SECURITY NOTE: It's safer to use environment variables. We keep the variables here
# to match your original script, but consider replacing with os.getenv(...),
# e.g., username = os.getenv("INPI_USERNAME").
username = "emile.rogier@hec.edu"   # Your INPI login email
password = "Hippolyte=12"            # Your INPI password (avoid hard-coding in real use)

# SIREN is the 9‑digit company identifier in France (no spaces)
siren = "834816985"

# INPI API endpoints used by this script
login_url = "https://registre-national-entreprises.inpi.fr/api/sso/login"
attachments_url = f"https://registre-national-entreprises.inpi.fr/api/companies/{siren}/attachments"

# We keep the login token in a small text file so we don't have to log in every time
# (until it expires). This is placed next to the script.
token_file = "inpi_token.txt"

# ------------------------------------------
# 2) Which document types should we download?
# ------------------------------------------
# The INPI returns many kinds of documents (called "actes"). Here we list
# the ones we want. We normalize text (accents/uppercase) so matching is reliable.
#
# You can extend this list with more types if needed.
allowed_types = [
    "statuts constitutifs",           # initial company bylaws
    "attestation de depot des fonds",# certificate of funds deposit
    "attestation bancaire"            # bank attestation
]

# -----------------------------
# 3) Small helper: normalize text
# -----------------------------
# This removes accents (é -> e), converts to plain ASCII and to lowercase.
# It ensures we can compare text safely even if INPI uses accents or capitals.
def normalize(text):
    return unicodedata.normalize('NFKD', text or "").encode('ascii', 'ignore').decode().lower()

# --------------------------------------------------------
# 4) Token storage helpers: save/load/clear the login token
# --------------------------------------------------------
# The INPI gives us a temporary token after login. We save it locally to avoid
# logging in again for every request.

def save_token(token):
    with open(token_file, "w") as f:
        f.write(token)

def load_token():
    if os.path.exists(token_file):
        with open(token_file, "r") as f:
            return f.read().strip()
    return None

def clear_token():
    # Delete the stored token file (used when the token is expired/invalid)
    if os.path.exists(token_file):
        os.remove(token_file)

# ----------------------
# 5) Log in and get token
# ----------------------
# We send our username/password to the INPI login API.
# If it's successful, we receive a short‑lived token that proves who we are.

def login():
    payload = {"username": username, "password": password}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(login_url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()  # raise an error if HTTP status is not 2xx
        token = response.json().get("token")  # the token is inside the JSON reply
        if not token:
            print("❌ Login failed: No token received")
            return None
        save_token(token)
        print("✅ Login successful. Token obtained and saved.")
        return token
    except requests.RequestException as e:
        # Network error, wrong URL, server trouble, etc.
        print(f"❌ Login failed: {e}")
        return None

# -----------------------------------------
# 6) Get a usable token (load or fresh login)
# -----------------------------------------
# Try to reuse an existing token from the file. If there is none, log in.

def get_token():
    token = load_token()
    if token:
        print("ℹ️ Using saved token.")
        return token
    return login()

# ------------------------------------
# 7) Ask INPI for the list of documents
# ------------------------------------
# With a valid token, we request all "attachments" (documents) for the SIREN.
# If the token has expired, we automatically log in again and retry once.

def fetch_attachments(token):
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(attachments_url, headers=headers, timeout=10)
        if response.status_code == 401:  # 401 = unauthorized (likely token expired)
            print("⚠️ Token expired or unauthorized, retrying login...")
            clear_token()
            new_token = login()
            if not new_token:
                return None
            return fetch_attachments(new_token)  # try again with new token
        response.raise_for_status()
        return response.json()  # this is a big JSON object containing the documents
    except requests.RequestException as e:
        print(f"❌ Failed to get attachments: {e}")
        return None

# --------------------------------------------
# 8) Keep only the document types we care about
# --------------------------------------------
# The JSON contains a list named "actes". Each item has one or more type labels.
# We normalize those labels and check whether any contains the allowed types.

def filter_actes(attachments_json):
    actes = attachments_json.get("actes", [])
    filtered = []
    for acte in actes:
        type_rdd_list = acte.get("typeRdd", [])
        # Extract and normalize all labels for this document
        acte_types = [normalize(t.get("typeActe", "")) for t in type_rdd_list]
        # If ANY normalized label contains ANY allowed type, we keep this document
        if any(any(allowed in acte_type for allowed in allowed_types) for acte_type in acte_types):
            filtered.append(acte)
    return filtered

# -------------------------------------------
# 9) Download one document (PDF) to your disk
# -------------------------------------------
# For each selected document ("acte"), we call the INPI download API and save it as <id>.pdf

def download_acte(token, acte):
    acte_id = acte.get("id")
    acte_name = acte.get("nomDocument", "unknown")
    download_url = f"https://registre-national-entreprises.inpi.fr/api/actes/{acte_id}/download"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        print(f"⬇️ Downloading acte: {acte_name} (ID: {acte_id})")
        response = requests.get(download_url, headers=headers, timeout=20)
        if response.status_code == 401:
            # If our token became invalid mid‑way, log in again and retry once
            print("⚠️ Token expired during download, retrying login...")
            clear_token()
            new_token = login()
            if not new_token:
                print("❌ Cannot download, login failed.")
                return
            download_acte(new_token, acte)
            return
        response.raise_for_status()
        filename = f"{acte_id}.pdf"
        with open(filename, "wb") as f:
            f.write(response.content)
        print(f"✅ Saved {filename}")
    except requests.RequestException as e:
        print(f"❌ Failed to download {acte_id}: {e}")

# ------------------
# 10) The main recipe
# ------------------
# Step-by-step:
#   a) Get a token (load or log in)
#   b) Ask INPI for the list of documents
#   c) Filter to only the document types we want
#   d) Download each selected document as a PDF

def main():
    token = get_token()
    if not token:
        print("❌ Cannot proceed without token.")
        return

    attachments = fetch_attachments(token)
    if not attachments:
        print("❌ No attachments found or failed to fetch.")
        return

    actes_filtered = filter_actes(attachments)
    print(f"📄 Found {len(actes_filtered)} acte(s) matching criteria.")

    for acte in actes_filtered:
        download_acte(token, acte)

    print("✅ Script complete.")

# Run the script only if this file is executed directly (not when imported)
if __name__ == "__main__":
    main()
