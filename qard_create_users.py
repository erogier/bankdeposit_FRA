#!/usr/bin/env python3
import os, sys, time, argparse, requests
from typing import List, Dict, Optional, Any

API_BASE = os.getenv("QARD_BASE", "https://api-demo.qardfinance.com")
API_KEY  = os.getenv("QARD_API_KEY", "19ac7ba25eadf4c16d1ed56b98ef5da29691e388ff88426570dc1a954f6360ab")  # or hardcode below
# API_KEY = "PUT_YOUR_QARD_API_KEY_HERE"

UA = "qard-company-profile-check/1.0"
CALL_PAUSE = 0.12
AUTO_CREATE_USERS = True  # set False if you prefer to skip instead of creating

def h_json():
    return {"accept":"application/json","X-API-KEY":API_KEY,"User-Agent":UA}

def h_json_ct():
    return {"accept":"application/json","content-type":"application/json","X-API-KEY":API_KEY,"User-Agent":UA}

def get_json(sess: requests.Session, path: str, params: dict=None, timeout: int=25):
    url = f"{API_BASE.rstrip('/')}{path}"
    r = sess.get(url, headers=h_json(), params=params or {}, timeout=timeout)
    try:
        data = r.json()
    except Exception:
        data = {"_text": r.text}
    return r.status_code, r.headers, data

def post_json(sess: requests.Session, path: str, payload: dict, timeout: int=25):
    url = f"{API_BASE.rstrip('/')}{path}"
    r = sess.post(url, headers=h_json_ct(), json=payload, timeout=timeout)
    try:
        data = r.json()
    except Exception:
        data = {"_text": r.text}
    return r.status_code, r.headers, data

def _print_http_error(prefix: str, code: int, data: dict, headers: dict):
    err_code = (data or {}).get("error_code")
    err_msg  = (data or {}).get("error_message") or (data or {}).get("message") or ""
    err_det  = (data or {}).get("error_details")
    retry_after = (headers or {}).get("Retry-After")
    print(f"{prefix}: {code}")
    if err_code: print(f"   • error_code: {err_code}")
    if err_msg:  print(f"   • error_message: {err_msg}")
    if err_det:  print(f"   • error_details: {err_det}")
    if not (err_code or err_msg or err_det):
        print(f"   • payload: {str(data)[:300]}")
    if retry_after:
        print(f"   • Retry-After: {retry_after}")

def auth_check(sess: requests.Session) -> bool:
    code, _, me = get_json(sess, "/api/v6/clients")
    if code == 200:
        print(f"✅ Auth OK. Client: {(me or {}).get('name','unknown')}")
        return True
    print(f"❌ Auth failed: {code} {me}")
    return False

def load_sirens(path: str) -> List[str]:
    out = []
    with open(path, "r") as f:
        for line in f:
            s = line.strip().replace(" ", "")
            if len(s) == 9 and s.isdigit():
                out.append(s)
    return out

def find_user_by_siren(sess: requests.Session, siren: str) -> Optional[Dict]:
    # Prefer server-side filter; fallback to scan a couple pages
    code, _, data = get_json(sess, "/api/v6/users", params={"siren": siren, "per_page": 50})
    if code == 200 and isinstance(data, dict) and isinstance(data.get("result"), list):
        for u in data["result"]:
            if str(u.get("siren","")).strip() == siren:
                return u
    for page in (1, 2, 3):
        code, _, data = get_json(sess, "/api/v6/users", params={"per_page": 100, "page": page})
        if code != 200 or not isinstance(data, dict):
            break
        for u in (data.get("result") or []):
            if str(u.get("siren","")).strip() == siren:
                return u
        cur = int(data.get("current_page", page)); last = int(data.get("last_page", cur))
        if cur >= last: break
    return None

def create_legal_user(sess: requests.Session, siren: str, name: Optional[str] = None, group: str = "default") -> Optional[Dict]:
    payload = {"name": name or f"SIREN {siren}", "siren": siren, "group": group}
    code, _, data = post_json(sess, "/api/v6/users/legal", payload)
    if code in (200, 201):
        return data
    _print_http_error(f"   ❌ Create user failed for {siren}", code, data, {})
    return None

# ---------- Company Profile ----------

def list_user_company_profile(sess: requests.Session, user_id: str) -> Optional[Dict]:
    """
    Company Legal FR → Company Profile endpoint:
      GET /api/v6/users/{userId}/company-profile
    Returns a single object with legal/identity info.
    """
    code, headers, data = get_json(sess, f"/api/v6/users/{user_id}/company-profile")
    if code != 200:
        _print_http_error("   ⚠️ Failed to fetch company profile", code, data, headers)
        return None
    if isinstance(data, dict):
        return data
    # Some tenants might wrap as {"result": {...}}
    if isinstance(data, dict) and isinstance(data.get("result"), dict):
        return data["result"]
    return None

def trigger_sync(sess: requests.Session, user_id: str, data_types=None) -> bool:
    """Start a sync; returns True if accepted (200/201/202)."""
    payload = {"data_types": data_types or ["COMPANY_PROFILE"]}
    code, headers, data = post_json(sess, f"/api/v6/users/{user_id}/sync", payload)
    if code in (200, 201, 202):
        msg = (data or {}).get("message") or "Sync launched"
        print(f"   🔄 {msg} (HTTP {code}).")
        return True
    _print_http_error("   ❌ Sync request failed", code, data, headers)
    return False

def wait_for_sync(sess: requests.Session, user_id: str, timeout_s: int = 120, poll_every_s: int = 3) -> str:
    """
    Poll recent syncs until latest is terminal.
    Returns 'SUCCESS', 'FAILED', 'CANCELED', 'TIMEOUT', or 'UNKNOWN'.
    """
    import time as _t
    deadline = _t.time() + timeout_s
    last_status = "UNKNOWN"
    while _t.time() < deadline:
        code, headers, data = get_json(sess, f"/api/v6/users/{user_id}/sync", params={"per_page": 1})
        items = []
        if code == 200:
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get("result") or []
        else:
            _print_http_error("   ⚠️ Could not read sync history", code, data, headers)

        if items:
            s = items[0]
            status = (s.get("status") or "").upper()
            last_status = status or last_status
            if status in ("SUCCESS", "FAILED", "CANCELED"):
                print(f"   ✅ Sync finished with status={status}.")
                return status
            elif status in ("PENDING", "RUNNING"):
                print("   ⏳ Sync in progress…")
        else:
            print("   ℹ️ No sync record yet; waiting…")
        _t.sleep(poll_every_s)
    print("   ⏱️ Sync wait timeout.")
    return "TIMEOUT"

def _fmt_headquarter(hq: Any) -> str:
    """Return a short one-line string from headquarter (corporate office) object if present."""
    if not isinstance(hq, dict):
        return ""
    parts = []
    for k in ("address", "postal_code", "city", "country"):
        v = hq.get(k)
        if v:
            parts.append(str(v))
    return ", ".join(parts)

def main():
    parser = argparse.ArgumentParser(description="Fetch Company Profile for users from SIRENs (Company Legal FR)")
    parser.add_argument("--sirens-file", default="sirens_existing_diffusible.txt",
                        help="Path to a text file with one 9-digit SIREN per line")
    parser.add_argument("--limit-users", type=int, default=50,
                        help="Process only the first N SIRENs (default 50)")
    parser.add_argument("--wait-sync", action="store_true",
                        help="Wait for COMPANY_PROFILE sync to finish before reading")
    args = parser.parse_args(args=[] if hasattr(sys, 'ps1') or 'PYCHARM_HOSTED' in os.environ else None)

    if not API_KEY:
        print("❌ Missing API key. Set QARD_API_KEY or hardcode API_KEY in this script.")
        sys.exit(2)

    sirens = load_sirens(args.sirens_file)
    if not sirens:
        print(f"❌ No valid SIRENs found in {args.sirens_file}")
        sys.exit(1)
    if args.limit_users > 0:
        sirens = sirens[:args.limit_users]

    sess = requests.Session()
    if not auth_check(sess):
        sys.exit(1)

    total_users = 0
    profiles_ok  = 0
    missing_users = []
    failures = 0

    for idx, siren in enumerate(sirens, 1):
        print(f"\n[{idx}/{len(sirens)}] SIREN {siren}")

        user = find_user_by_siren(sess, siren)
        if not user and AUTO_CREATE_USERS:
            user = create_legal_user(sess, siren)
            if user:
                print(f"   👤 Created user id={user.get('id')} name={user.get('name')}")
            time.sleep(CALL_PAUSE)

        if not user:
            print("   ❌ No user found. Skipping.")
            missing_users.append(siren)
            continue

        uid = user.get("id")
        uname = user.get("name") or user.get("display_name") or "unknown"
        print(f"   ✅ User: {uname} (id={uid})")
        total_users += 1

        # Trigger a COMPANY_PROFILE sync (accepted = 200/201/202)
        if trigger_sync(sess, uid, data_types=["COMPANY_PROFILE"]) and args.wait_sync:
            wait_for_sync(sess, uid, timeout_s=180, poll_every_s=4)

        # Fetch company profile
        prof = list_user_company_profile(sess, uid)
        if not prof:
            print("   — no company profile —")
            failures += 1
            time.sleep(CALL_PAUSE)
            continue

        # Print key fields (only those available)
        name = prof.get("name") or ""
        reg_num = prof.get("registration_number") or ""   # SIREN for FR
        reg_date = prof.get("registration_date") or ""
        rncs_reg_date = prof.get("rncs_registration_date") or ""
        vat = prof.get("vat_number") or ""
        staff = prof.get("staff") or ""
        staff_year = prof.get("staff_year")
        legal = prof.get("legal") or {}
        legal_form = legal.get("form") or ""
        person_type = legal.get("person_type") or ""
        reg_court = prof.get("registration_court") or {}
        rc_name = reg_court.get("name") or ""
        rc_code = reg_court.get("code") or ""
        capital = prof.get("capital") or {}
        cap_amt = capital.get("amount")
        cap_cur = (capital.get("currency") or {}).get("code") if isinstance(capital.get("currency"), dict) else capital.get("currency")
        hq = _fmt_headquarter(prof.get("headquarter"))

        print("   🧾 Company Profile:")
        print(f"      • name: {name}")
        print(f"      • registration_number: {reg_num}")
        if reg_date:       print(f"      • registration_date: {reg_date}")
        if rncs_reg_date:  print(f"      • rncs_registration_date: {rncs_reg_date}")
        if vat:            print(f"      • vat_number: {vat}")
        if legal_form or person_type:
            print(f"      • legal: form={legal_form} person_type={person_type}")
        if rc_name or rc_code:
            print(f"      • registration_court: name={rc_name} code={rc_code}")
        if cap_amt is not None or cap_cur:
            print(f"      • capital: amount={cap_amt} currency={cap_cur}")
        if staff:
            print(f"      • staff: {staff}" + (f" (year {staff_year})" if staff_year else ""))
        if hq:
            print(f"      • headquarter: {hq}")

        profiles_ok += 1
        time.sleep(CALL_PAUSE)

    print("\n===== SUMMARY =====")
    print(f"Users processed:         {total_users}")
    print(f"Company profiles found:  {profiles_ok}")
    if missing_users:
        print(f"Missing users ({len(missing_users)}): {', '.join(missing_users[:20])}{' ...' if len(missing_users)>20 else ''}")
    if failures:
        print(f"Profiles not available:  {failures}")

if __name__ == "__main__":
    main()
