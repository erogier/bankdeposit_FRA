#!/usr/bin/env python3
import os
import sys
import requests

API_BASE = os.getenv("QARD_BASE", "https://api-demo.qardfinance.com")
API_KEY  = os.getenv("QARD_API_KEY", "19ac7ba25eadf4c16d1ed56b98ef5da29691e388ff88426570dc1a954f6360ab")

def get(path, params=None):
    url = f"{API_BASE.rstrip('/')}{path}"
    headers = {
        "accept": "application/json",
        "X-API-KEY": API_KEY,
        "User-Agent": "qard-smoketest/1.0",
    }
    r = requests.get(url, headers=headers, params=params, timeout=15)
    print(f"GET {url} -> {r.status_code}")
    try:
        print((r.json() if r.headers.get("content-type","").lower().startswith("application/json") else r.text)[:500])
    except Exception:
        print(r.text[:500])
    return r

def main():
    if not API_KEY:
        print("❌ Missing API key. Set QARD_API_KEY env var or edit the script.")
        sys.exit(2)

    # 1) Basic authenticated call shown in docs
    r = get("/api/v6/clients")
    if r.status_code == 200:
        print("✅ Auth OK on /api/v6/clients")
    elif r.status_code in (401, 403):
        print("❌ Auth failed (check X-API-KEY and environment).")
        sys.exit(1)

    # 2) Optional: try a users endpoint referenced in docs
    get("/api/v6/users")  # may be 200/403 depending on your tenant/permissions

    # 3) Optional: try sorting (docs: sort_field + direction=ASC|DESC)
    get("/api/v6/clients", params={"sort_field":"source_created_at","direction":"ASC"})  # may be ignored if endpoint doesn't support sorting

if __name__ == "__main__":
    main()

