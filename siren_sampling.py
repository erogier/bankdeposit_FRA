"""
API_KEY    = "bc13cd61-47c9-4ca3-93cd-6147c94ca3b3"
Fast 'diffusible-only' SIREN existence check:
- Draw K random *valid* SIRENs within [RANGE_START, RANGE_END] (Luhn).
- Query SIRENE search endpoint in batches with OR'ed siren terms.
- Count how many are diffusible (i.e., returned by the search API).
- Writes found SIRENs to 'sirens_existing_diffusible.txt'.

NOTE: Non-diffusible entities (HTTP 403 in item lookup) do NOT appear here.
"""

import random
import time
import requests
from typing import List, Set
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------- CONFIG (edit here) -----------
API_KEY    = "bc13cd61-47c9-4ca3-93cd-6147c94ca3b3"  # INSEE portal "Integration" key (header X-INSEE-Api-Key-Integration)
BASE_SEARCH_URL = "https://api.insee.fr/api-sirene/3.11/siren"

SAMPLE_SIZE = 1_000
RANGE_START = 821_000_000
RANGE_END   = 990_000_000   # inclusive

BATCH_SIZE  = 80            # ~80 keeps URL q under a few KB
CONNECT_TO  = 6             # connect timeout (s)
READ_TO     = 18            # read timeout (s)
RETRY_TOTAL = 3             # retries on 429/5xx/timeouts
PAUSE_BETWEEN_BATCHES = 0.05  # polite tiny delay between requests
# ------------------------------------------

# ---------- Luhn helpers for SIREN ----------
def siren_check_digit(prefix8: str) -> int:
    """Compute Luhn check digit for an 8-digit prefix (SIREN)."""
    total, alt = 0, True  # rightmost of the 8 is doubled (check digit will be to its right)
    for ch in reversed(prefix8):
        d = int(ch)
        if alt:
            d *= 2
            if d > 9:
                d -= 9
        total += d
        alt = not alt
    return (10 - (total % 10)) % 10

def make_siren_from_prefix8(prefix8: int) -> int:
    s8 = f"{prefix8:08d}"
    return int(s8 + str(siren_check_digit(s8)))

def sample_valid_sirens_in_range(start9: int, end9: int, k: int) -> List[int]:
    """
    Uniformly sample k distinct *valid* SIRENs inside [start9, end9].
    We uniformly draw 8-digit prefixes over [floor(L/10)..floor(U/10)],
    compute their Luhn check digit, and accept if the 9-digit SIREN lies in range.
    """
    if len(str(start9)) != 9 or len(str(end9)) != 9 or start9 > end9:
        raise ValueError("start/end must be 9-digit integers with start <= end.")
    p8_lo, p8_hi = start9 // 10, end9 // 10

    out, seen = [], set()
    # rejection only near numeric edges; this converges quickly for large ranges
    while len(out) < k:
        prefix = random.randint(p8_lo, p8_hi)
        s = make_siren_from_prefix8(prefix)
        if start9 <= s <= end9 and s not in seen:
            seen.add(s)
            out.append(s)
    return out

# ---------- HTTP session with retries ----------
def make_session(api_key: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "X-INSEE-Api-Key-Integration": api_key,
        "Accept": "application/json",
        "User-Agent": "sirene-batch-diffusible/1.0"
    })
    retry = Retry(
        total=RETRY_TOTAL, connect=RETRY_TOTAL, read=RETRY_TOTAL,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=32)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

# ---------- Batch query (diffusible only) ----------
def batch_diffusible_exists(sess: requests.Session, chunk: List[int]) -> Set[str]:
    """
    Query the search endpoint with an OR of siren terms:
        q = (siren:XXXXXXXXX OR siren:YYYYYYYYY ...)
    Returns the set of diffusible SIRENs (as zero-padded strings) found in this chunk.
    """
    # Build q
    terms = " OR ".join(f"siren:{x:09d}" for x in chunk)
    q = f"({terms})"
    params = {
        "q": q,
        "champs": "siren",         # minimal payload
        "nombre": len(chunk)       # ≤ 1000, so no pagination needed
    }
    r = sess.get(BASE_SEARCH_URL, params=params, timeout=(CONNECT_TO, READ_TO))
    if not r.ok:
        # Soft-fail: return empty set for this chunk
        # (You can print diagnostics if needed: print(r.status_code, r.text[:200]))
        return set()
    data = r.json()
    return {
        ul.get("siren")
        for ul in data.get("unitesLegales", [])
        if ul.get("siren")
    }

def main():
    if not API_KEY or API_KEY == "YOUR_REAL_KEY":
        raise SystemExit("Please set API_KEY to your real INSEE key (X-INSEE-Api-Key-Integration).")

    # 1) Draw valid SIRENs in the numeric window
    sirens = sample_valid_sirens_in_range(RANGE_START, RANGE_END, SAMPLE_SIZE)
    print(f"Generated {len(sirens)} valid SIRENs in range [{RANGE_START}, {RANGE_END}].")

    # 2) Batch-check diffusible existence via the search endpoint
    sess = make_session(API_KEY)
    existing: Set[str] = set()

    for i in range(0, len(sirens), BATCH_SIZE):
        chunk = sirens[i:i + BATCH_SIZE]
        existing |= batch_diffusible_exists(sess, chunk)
        time.sleep(PAUSE_BETWEEN_BATCHES)  # be polite

    # 3) Report + save
    print(f"Checked {len(sirens)} candidates in {len(sirens)//BATCH_SIZE + 1} batches.")
    print(f"→ Diffusible existing (returned by search): {len(existing)}")

    with open("sirens_existing_diffusible.txt", "w") as f:
        f.write("\n".join(sorted(existing)))

    print("Wrote sirens_existing_diffusible.txt")

if __name__ == "__main__":
    main()