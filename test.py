import requests

try:
    r = requests.get("https://registre-national-entreprises.inpi.fr/api/sso/login", timeout=10)
    print("Status:", r.status_code)
except Exception as e:
    print("Error:", e)