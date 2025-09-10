#!/usr/bin/env python3
import os, re, csv, time, requests
from typing import List, Dict, Optional

API_BASE = os.getenv("QARD_BASE", "https://api-demo.qardfinance.com")
API_KEY  = os.getenv("QARD_API_KEY", "19ac7ba25eadf4c16d1ed56b98ef5da29691e388ff88426570dc1a954f6360ab")
UA       = "qard-user-conn-check/1.0"
CALL_PAUSE = 0.15

CSV_PATH = "/Users/emilerogier/Documents/PhD/Research projects/Advisory banks/acts_results.csv"
REPORT_PATH = "/Users/emilerogier/Documents/PhD/Research projects/Advisory banks/user_connections_report.csv"

# ---------- HTTP helpers ----------
def json_headers():
    return {"accept":"application/json","X-API-KEY":API_KEY,"User-Agent":UA}

def get_json(sess: requests.Session, path: str, params: dict = None, timeout: int = 20):
    url = f"{API_BASE.rstrip('/')}{path}"
    r = sess.get(url, headers=json_headers(), params=params or {}, timeout=timeout)
    try:
        data = r.json()
    except Exception:
        data = {"_text": r.text}
    return r.status_code, r.headers, data

# ---------- API helpers ----------
def auth_check(sess: requests.Session) -> bool:
    code, _, me = get_json(sess, "/api/v6/clients")
    if code == 200:
        name = (me or {}).get("name", "unknown")
        print(f"✅ Auth OK. Client: {name}")
        return True
    print(f"❌ Auth failed on /api/v6/clients: {code} {me}")
    return False

def get_user(sess: requests.Session, user_id: str) -> Optional[Dict]:
    code, _, data = get_json(sess, f"/api/v6/users/{user_id}")
    if code == 200 and isinstance(data, dict):
        return data
    return None

def list_data_connections(sess: requests.Session, user_id: str) -> List[Dict]:
    # A) Nested route
    code, _, data = get_json(sess, f"/api/v6/users/{user_id}/data-connections", params={"per_page": 100})
    if code == 200:
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in ("result", "data_connections", "connections"):
                if isinstance(data.get(k), list):
                    return data[k]
    # B) Fallback flat
    code, _, data = get_json(sess, "/api/v6/data-connections", params={"user_id": user_id, "per_page": 100})
    if code == 200:
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in ("result", "data_connections", "connections"):
                if isinstance(data.get(k), list):
                    return data[k]
    return []

# ---------- Input ----------
def load_user_ids_from_csv(path: str) -> List[str]:
    if not os.path.exists(path):
        print(f"❌ CSV not found: {path}")
        return []
    with open(path, newline="", encoding="utf-8") as f:
        sample = f.read(4096); f.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=";,")
        reader = csv.DictReader(f, dialect=dialect)
        ids = []
        for row in reader:
            uid = (row.get("user_id") or row.get("id") or "").strip()
            if uid:
                ids.append(uid)
        return ids

# ---------- Main ----------
def main():
    if not API_KEY:
        print("❌ Missing API key. Set QARD_API_KEY env var.")
        return

    user_ids = load_user_ids_from_csv(CSV_PATH)
    if not user_ids:
        print("❌ No user IDs found in CSV.")
        return

    sess = requests.Session()
    if not auth_check(sess):
        return

    results = []
    print(f"\n📄 Checking {len(user_ids)} users from {CSV_PATH}")

    for i, uid in enumerate(user_ids, 1):
        print(f"\n[{i}/{len(user_ids)}] User {uid}")
        user = get_user(sess, uid)
        if not user:
            print("   ❌ User not found or inaccessible.")
            results.append({"user_id": uid, "exists": False, "active": False, "connections": ""})
            time.sleep(CALL_PAUSE); continue

        utype = (user.get("type") or "").upper()
        name  = user.get("name") or f"{user.get('first_name','')} {user.get('last_name','')}".strip()
        siren = user.get("siren") or ""
        print(f"   ✅ Exists ({utype}) name='{name}' siren='{siren}'")

        conns = list_data_connections(sess, uid)
        print(f"   🔌 Data connections: {len(conns)}")

        summary_items = []
        any_connected = False
        for c in conns:
            provider = c.get("provider_name") or c.get("provider") or "?"
            status   = (c.get("status") or "?").upper()
            summary_items.append(f"{provider}:{status}")
            if status == "CONNECTED":
                any_connected = True

        is_active = any_connected
        print(f"   ACTIVE={is_active} connections=[{', '.join(summary_items) if summary_items else '-'}]")

        results.append({
            "user_id": uid,
            "exists": True,
            "type": utype,
            "name": name,
            "siren": siren,
            "active": is_active,
            "connections": "; ".join(summary_items) if summary_items else ""
        })

        time.sleep(CALL_PAUSE)

    # Save report
    fieldnames = ["user_id","exists","type","name","siren","active","connections"]
    with open(REPORT_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            row = {k: r.get(k, "") for k in fieldnames}
            w.writerow(row)

    print(f"\n📝 Report written to: {REPORT_PATH}")

if __name__ == "__main__":
    main()
