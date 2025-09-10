#!/usr/bin/env python3
import os, time, requests, sys, csv, pathlib

# ====== CONFIG ======
API_BASE   = os.getenv("QARD_BASE", "https://api-demo.qardfinance.com")  # demo by default
API_KEY    = os.getenv("QARD_API_KEY", "19ac7ba25eadf4c16d1ed56b98ef5da29691e388ff88426570dc1a954f6360ab")
UA         = "qard-acts-only/1.5"
SIRENS_FILE = "sirens_existing_diffusible.txt"

# Output spreadsheet (CSV)
OUTPUT_CSV = os.getenv("QARD_OUTPUT_CSV", "acts_results.csv")

# ====== HTTP HELPERS ======
H_JSON    = {"accept": "application/json", "X-API-KEY": API_KEY, "User-Agent": UA}
H_JSON_CT = {**H_JSON, "content-type": "application/json"}

def get(path, params=None):
    r = requests.get(f"{API_BASE.rstrip('/')}{path}", headers=H_JSON, params=params or {}, timeout=30)
    return r.status_code, r.headers, _safe_json(r)

def post(path, payload):
    r = requests.post(f"{API_BASE.rstrip('/')}{path}", headers=H_JSON_CT, json=payload, timeout=30)
    return r.status_code, r.headers, _safe_json(r)

def patch(path, payload):
    r = requests.patch(f"{API_BASE.rstrip('/')}{path}", headers=H_JSON_CT, json=payload, timeout=30)
    return r.status_code, r.headers, _safe_json(r)

def _safe_json(r):
    try:
        return r.json()
    except Exception:
        return {"_text": r.text}

def fail(msg, code=None, data=None):
    print(f"❌ {msg}" + (f" (HTTP {code})" if code else ""))
    if isinstance(data, dict):
        ec = data.get("error_code")
        em = data.get("error_message") or data.get("message")
        ed = data.get("error_details")
        if ec or em: print(f"   • {ec or ''} {em or ''}".strip())
        if ed: print(f"   • details: {ed}")
        else: print(f"   • payload: {str(data)[:300]}")
    raise SystemExit(1)

# ====== CSV HELPERS ======
CSV_FIELDS = ["siren", "user_id", "file_id", "titles", "date"]

def ensure_csv_header(path: str):
    """Create the CSV with header if it doesn't exist."""
    p = pathlib.Path(path)
    if not p.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()

def append_rows_to_csv(path: str, rows: list[dict]):
    if not rows:
        return
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        for row in rows:
            writer.writerow(row)

# ====== MAIN WORKFLOW PER SIREN ======
def process_siren(SIREN: str):
    print("\n" + "="*60)
    print(f"🔎 Processing SIREN {SIREN}")
    print("="*60)

    # A) USER (find or create)
    code, _, data = get("/api/v6/users", params={"siren": SIREN, "per_page": 1})
    user = None
    if code == 200 and isinstance(data, dict):
        arr = data.get("result") or []
        if arr: user = arr[0]
    if not user:
        code, _, data = post("/api/v6/users/legal", {"name": f"SIREN {SIREN}", "siren": SIREN, "group": "default"})
        if code not in (200, 201):
            print(f"❌ Failed to create user for {SIREN}")
            return
        user = data
        print(f"👤 Created user id={user.get('id')}")
    uid = user.get("id")
    print(f"✅ Using user id={uid}")

    # B) DATA CONNECTION (ensure provider exists; we won't try to narrow types here)
    code, _, dcs = get(f"/api/v6/users/{uid}/data-connections")
    if code != 200:
        print(f"❌ Could not list data connections for {SIREN}")
        return
    arr = dcs if isinstance(dcs, list) else (dcs.get("result") or [])
    dc = next((d for d in arr if (d.get("provider_name") or "").lower() == "company_legal_fr"), None)
    if not dc:
        payload = {"provider_name": "company_legal_fr", "requested_data_types": ["ACT"]}
        code, _, data = post(f"/api/v6/users/{uid}/data-connections", payload)
        if code not in (200, 201) and code != 409:
            print(f"❌ Failed to create data connection for {SIREN}")
            return
        if code == 409:
            code, _, dcs = get(f"/api/v6/users/{uid}/data-connections")
            arr = dcs if isinstance(dcs, list) else (dcs.get("result") or [])
            dc = next((d for d in arr if (d.get("provider_name") or "").lower() == "company_legal_fr"), None)
        else:
            dc = data
        print(f"🔌 Using company_legal_fr DC id={dc.get('id')}")
    else:
        print(f"🔌 Found company_legal_fr DC id={dc.get('id')} (status={dc.get('status')})")

    # C) SYNC only ACT
    code, _, sync = post(f"/api/v6/users/{uid}/sync", {"data_types": ["ACT"]})
    if code in (200, 201, 202):
        print("🔄 Sync launched. Waiting for completion…")
    elif code == 409:
        print("🔄 A sync is already running (409). Waiting for it to finish…")
    else:
        print(f"❌ Sync request failed for {SIREN} (HTTP {code})")
        return

    deadline = time.time() + 180
    last_status = None
    while time.time() < deadline:
        code, _, hist = get(f"/api/v6/users/{uid}/sync", params={"per_page": 1})
        if code == 200:
            items = hist if isinstance(hist, list) else (hist.get("result") or [])
            if items:
                status = (items[0].get("status") or "").upper()
                if status != last_status:
                    print(f"⏳ Sync status: {status}")
                    last_status = status
                if status in ("SUCCESS", "FAILED", "CANCELED"):
                    print(f"✅ Sync finished with status={status}")
                    break
        time.sleep(3)

    # D) Fetch ACTs and write rows to CSV
    code, _, acts = get(f"/api/v6/users/{uid}/acts", params={"per_page": 100})
    if code == 200:
        items = acts if isinstance(acts, list) else (acts.get("result") or acts.get("acts") or [])
        print(f"\n📄 ACTs for {SIREN}: {len(items)}")
        rows = []
        for it in items:
            rows.append({
                "siren"   : SIREN,
                "user_id" : uid,
                "file_id" : it.get("file_id"),
                "titles"  : "; ".join(it.get("titles") or []),
                "date"    : it.get("date"),
            })
            print(f"  • file_id: {it.get('file_id')} | titles: {it.get('titles') or []} | date: {it.get('date')}")
        append_rows_to_csv(OUTPUT_CSV, rows)
        print(f"🧾 Appended {len(rows)} row(s) to {OUTPUT_CSV}")
    elif code == 404:
        print(f"📄 No ACTs available for {SIREN} (404).")
    else:
        print(f"❌ Failed to fetch ACTs for {SIREN} (HTTP {code})")

# ====== ENTRYPOINT ======
if __name__ == "__main__":
    if not API_KEY:
        raise SystemExit("❌ Missing API key. Set QARD_API_KEY.")

    # Prepare CSV
    ensure_csv_header(OUTPUT_CSV)

    # Read SIRENs
    try:
        with open(SIRENS_FILE, "r", encoding="utf-8") as f:
            sirens = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"❌ File not found: {SIRENS_FILE}")
        sys.exit(1)

    # Auth once (check client)
    code, _, me = get("/api/v6/clients")
    if code != 200: fail("Auth check failed", code, me)
    print(f"✅ Auth OK. Client: {me.get('name', 'unknown')}")

    # Process each SIREN
    for siren in sirens:
        process_siren(siren)

    print(f"\n✅ Done. Results saved to: {OUTPUT_CSV}")
