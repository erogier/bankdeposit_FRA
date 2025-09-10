"""
INPI legal documents downloader (multi-SIREN)
- Reads SIRENs from 'sirens_existing_diffusible.txt' (one per line, 9 digits)
- Logs in once, reuses/refreshes token (re-login on 401 only)
- For each SIREN: fetch attachments, filter allowed types, download PDFs
- Respects per-minute pacing and stops cleanly on daily 429 quota
"""

import os
import requests
import unicodedata
import time
import threading
import random

# ---------------------------
# 1) Configuration
# ---------------------------

# 1a) Environment selection (prod / preprod)
USE_PREPROD = False
BASE = "https://registre-national-entreprises-pprod.inpi.fr" if USE_PREPROD else "https://registre-national-entreprises.inpi.fr"

# 1b) Credentials (prefer env vars)
username = os.getenv("INPI_USERNAME", "emile.rogier@hec.edu")
password = os.getenv("INPI_PASSWORD", "Hippolyte=12")
# If you want to enforce env vars only, uncomment:
# if not username or not password:
#     raise RuntimeError("Please set INPI_USERNAME and INPI_PASSWORD environment variables.")

sirens_file = "sirens_existing_diffusible.txt"   # one SIREN per line (9 digits)
token_file  = "inpi_token.txt"

login_url = f"{BASE}/api/sso/login"

# 1c) Documents fetched — allowed acte types (normalized, lowercase)
allowed_types = [
    "statuts constitutifs",
    "attestation de depot des fonds",
    "attestation bancaire",
]

# 1d) Politeness between SIRENs
PAUSE_BETWEEN_SIRENS_SEC = 1.00

# 1e) Per-group throughput caps (conservative vs. official)
JSON_RPM = 200   # safe under 250 req/min/jeton
DOCS_RPM = 40    # safe under 50 req/min/jeton

class RateLimiter:
    """
    Simple per-group min-interval limiter + tiny jitter to avoid bursts.
    """
    def __init__(self, rpm):
        self.min_interval = 60.0 / float(rpm)
        self.lock = threading.Lock()
        self.last_ts = 0.0

    def wait(self):
        with self.lock:
            now = time.time()
            jitter = random.uniform(0.0, 0.08)  # small desync
            next_ok = self.last_ts + self.min_interval + jitter
            sleep_for = max(0.0, next_ok - now)
            if sleep_for > 0:
                time.sleep(sleep_for)
            self.last_ts = max(now, next_ok)

json_limiter = RateLimiter(JSON_RPM)
docs_limiter = RateLimiter(DOCS_RPM)

# ---------------------------
# 2) Helpers
# ---------------------------
def normalize(text):
    return unicodedata.normalize('NFKD', text or "").encode('ascii', 'ignore').decode().lower()

def save_token(token):
    with open(token_file, "w") as f:
        f.write(token)

def load_token():
    if os.path.exists(token_file):
        with open(token_file, "r") as f:
            return f.read().strip()
    return None

def clear_token():
    if os.path.exists(token_file):
        os.remove(token_file)

# ---------------------------
# 3) Auth
# ---------------------------
def login():
    payload = {"username": username, "password": password}
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "inpi-actes-downloader/1.0"
    }
    try:
        resp = requests.post(login_url, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        token = resp.json().get("token")
        if not token:
            print("❌ Login failed: No token received")
            return None
        save_token(token)
        print("✅ Login successful. Token saved.")
        return token
    except requests.RequestException as e:
        print(f"❌ Login failed: {e}")
        return None

def get_token():
    token = load_token()
    if token:
        print("ℹ️ Using saved token.")
        return token
    return login()

# 3b) Token validation / refresh (uses BASE, respects JSON pacing)
def validate_or_refresh_token(token, test_siren):
    """
    Probe a protected endpoint with the given token.
    - If 200..299: token OK → return it.
    - If 401: refresh once via login → return new token (or None on failure).
    - If 429: daily quota reached → stop run cleanly.
    - Any other network error: try a clean login once and return new token/None.
    """
    url = f"{BASE}/api/companies/{test_siren}/attachments"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        json_limiter.wait()
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 401:
            print("⚠️ Saved token is unauthorized. Re-logging…")
            clear_token()
            return login()
        if r.status_code == 429:
            print("ℹ️ Token probe hit daily quota (429). Stopping.")
            raise SystemExit(2)
        r.raise_for_status()
        return token
    except requests.RequestException as e:
        print(f"ℹ️ Token probe failed ({e}). Trying fresh login…")
        clear_token()
        return login()

# ---------------------------
# 4) API calls
# ---------------------------
def fetch_attachments(token, siren, max_attempts=4):
    """
    Fetch attachments with retry/backoff on transient server errors (5xx).
    Re-login only on 401. On 429 (doc = daily quota), stop cleanly.
    Returns: (json_or_none, token)
    """
    attachments_url = f"{BASE}/api/companies/{siren}/attachments"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "inpi-actes-downloader/1.0"
    }
    backoffs = [3, 8, 20]  # for 5xx / transient errors

    for attempt in range(1, max_attempts + 1):
        try:
            json_limiter.wait()
            r = requests.get(attachments_url, headers=headers, timeout=20)

            if r.status_code == 401:
                print("⚠️ Token expired/unauthorized, retrying login…")
                clear_token()
                new_token = login()
                if not new_token:
                    return None, None
                token = new_token
                headers["Authorization"] = f"Bearer {token}"
                continue

            if r.status_code == 429:
                print(f"⛔ Daily quota exceeded (429) while fetching metadata for {siren}. Stopping.")
                raise SystemExit(2)

            if r.status_code == 403:
                print(f"🚫 Forbidden (403) for {siren}. Skipping.")
                return None, token

            if r.status_code >= 500:
                if attempt < max_attempts:
                    wait = backoffs[min(attempt - 1, len(backoffs) - 1)]
                    jitter = random.uniform(0.5, 1.5)
                    print(f"🔁 Server {r.status_code} for {siren}. Retrying in {wait + jitter:.1f}s (attempt {attempt}/{max_attempts})…")
                    time.sleep(wait + jitter)
                    continue
                else:
                    print(f"❌ Failed to get attachments for {siren}: {r.status_code} after {max_attempts} attempts")
                    return None, token

            r.raise_for_status()
            return r.json(), token

        except requests.ConnectionError as e:
            if attempt < max_attempts:
                wait = backoffs[min(attempt - 1, len(backoffs) - 1)]
                jitter = random.uniform(0.5, 1.5)
                print(f"🔌 Connection error for {siren}: {e}. Retrying in {wait + jitter:.1f}s (attempt {attempt}/{max_attempts})…")
                time.sleep(wait + jitter)
                continue
            else:
                print(f"❌ Failed to get attachments for {siren}: {e}")
                return None, token

        except requests.RequestException as e:
            if attempt < max_attempts:
                wait = backoffs[min(attempt - 1, len(backoffs) - 1)]
                jitter = random.uniform(0.5, 1.5)
                print(f"⚠️ HTTP error for {siren}: {e}. Retrying in {wait + jitter:.1f}s (attempt {attempt}/{max_attempts})…")
                time.sleep(wait + jitter)
                continue
            else:
                print(f"❌ Failed to get attachments for {siren}: {e}")
                return None, token

    return None, token

def filter_actes(attachments_json):
    """
    Keep only wanted types, skip deleted/non-public as per API doc.
    """
    actes = attachments_json.get("actes", [])
    filtered = []
    for acte in actes:
        # Skip deleted
        if acte.get("deleted") is True:
            continue
        # Skip non-public if confidentiality present
        conf = acte.get("confidentiality")
        if isinstance(conf, str) and conf.lower() != "public":
            continue

        type_rdd_list = acte.get("typeRdd", [])
        acte_types = [normalize(t.get("typeActe", "")) for t in type_rdd_list]
        if any(any(allowed in acte_type for allowed in allowed_types) for acte_type in acte_types):
            filtered.append(acte)
    return filtered

def download_acte(token, siren, acte, max_attempts=4):
    """
    Download a single acte PDF.
    - 401 → re-login once
    - 403 → skip
    - 429 (doc: daily quota) → stop run cleanly
    - 5xx / transient → backoff & retry
    """
    import email.utils as eut, datetime as dt

    acte_id = acte.get("id")
    acte_name = acte.get("nomDocument", "unknown")
    url = f"{BASE}/api/actes/{acte_id}/download"
    headers = {"Authorization": f"Bearer {token}", "User-Agent": "inpi-actes-downloader/1.0"}

    base_backoffs = [8, 20, 45]  # for transient errors

    def _retry_after_seconds(resp):
        ra = resp.headers.get("Retry-After")
        if not ra:
            return 0
        try:
            return int(ra)
        except ValueError:
            try:
                when = eut.parsedate_to_datetime(ra)
                if when.tzinfo is None:
                    when = when.replace(tzinfo=dt.timezone.utc)
                now = dt.datetime.now(dt.timezone.utc)
                return max(0, int((when - now).total_seconds()))
            except Exception:
                return 0

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"   ⬇️  {siren}: {acte_name} (ID {acte_id})")
            docs_limiter.wait()
            r = requests.get(url, headers=headers, timeout=30)

            if r.status_code == 401:
                print("   ⚠️  Token expired during download, re-logging…")
                clear_token()
                new_token = login()
                if not new_token:
                    print("   ❌ Cannot download, login failed.")
                    return token
                token = new_token
                headers["Authorization"] = f"Bearer {token}"
                continue

            if r.status_code == 429:
                # Per doc, 429 is daily quota exceeded → stop run cleanly
                ra = _retry_after_seconds(r)
                if ra:
                    print(f"   ⛔ Daily quota exceeded (429). Retry-After: {ra}s. Stopping.")
                else:
                    print(f"   ⛔ Daily quota exceeded (429). Stopping.")
                raise SystemExit(2)

            if r.status_code == 403:
                print(f"   🚫 Forbidden (403) for {acte_id}. Skipping.")
                return token

            if r.status_code >= 500:
                if attempt < max_attempts:
                    wait = base_backoffs[min(attempt - 1, len(base_backoffs) - 1)]
                    jitter = random.uniform(0.5, 1.5)
                    print(f"   🔁 Server {r.status_code}. Retrying in {wait + jitter:.1f}s (attempt {attempt}/{max_attempts})…")
                    time.sleep(wait + jitter)
                    continue
                else:
                    print(f"   ❌ Failed to download {acte_id}: server {r.status_code} after {max_attempts} attempts.")
                    return token

            r.raise_for_status()

            filename = f"{siren}_{acte_id}.pdf"
            with open(filename, "wb") as f:
                f.write(r.content)
            print(f"   ✅  Saved {filename}")

            # Polite pause after a successful download (helps keep DOCS RPM low)
            time.sleep(1.0)
            return token

        except requests.RequestException as e:
            # Other transient errors
            if attempt < max_attempts:
                wait = base_backoffs[min(attempt - 1, len(base_backoffs) - 1)]
                jitter = random.uniform(0.3, 1.2)
                print(f"   ⚠️  Download error {e}. Retrying in {wait + jitter:.1f}s (attempt {attempt}/{max_attempts})…")
                time.sleep(wait + jitter)
                continue
            else:
                print(f"   ❌ Failed to download {acte_id}: {e}")
                return token

    return token

# ---------------------------
# 5) I/O for SIREN list
# ---------------------------
def load_sirens(path):
    sirens = []
    if not os.path.exists(path):
        print(f"❌ SIREN list file not found: {path}")
        return sirens
    with open(path, "r") as f:
        for line in f:
            s = line.strip().replace(" ", "")
            if len(s) == 9 and s.isdigit():
                sirens.append(s)
            else:
                if s:
                    print(f"⚠️ Skipping invalid SIREN '{s}' (must be 9 digits)")
    return sirens

# ---------------------------
# 6) Main
# ---------------------------
def main():
    # Load SIRENs to process
    sirens = load_sirens(sirens_file)
    if not sirens:
        print("❌ No valid SIRENs to process.")
        return

    token = get_token()
    if not token:
        print("❌ Cannot proceed without token.")
        return

    # Validate token against the first SIREN; refresh if needed
    token = validate_or_refresh_token(token, sirens[0])
    if not token:
        print("❌ Cannot proceed without valid token.")
        return

    total_found = 0
    total_downloaded = 0

    for idx, s in enumerate(sirens, 1):
        print(f"\n[{idx}/{len(sirens)}] SIREN {s}")
        attachments, token = fetch_attachments(token, s)
        if not attachments:
            print(f"   ❌ No attachments JSON for {s} (skip)")
            time.sleep(PAUSE_BETWEEN_SIRENS_SEC + random.uniform(0.0, 0.2))
            continue

        actes_filtered = filter_actes(attachments)
        print(f"   📄 Found {len(actes_filtered)} matching acte(s).")
        total_found += len(actes_filtered)

        for acte in actes_filtered:
            prev_token = token
            acte_id = acte.get("id")
            expected_filename = f"{s}_{acte_id}.pdf"

            token = download_acte(token, s, acte) or prev_token

            if os.path.exists(expected_filename):
                total_downloaded += 1

        # polite per-SIREN delay with small jitter
        time.sleep(PAUSE_BETWEEN_SIRENS_SEC + random.uniform(0.0, 0.2))

    print("\n✅ Done.")
    print(f"   Total matching actes: {total_found}")
    print(f"   Total files downloaded: {total_downloaded}")

if __name__ == "__main__":
    main()