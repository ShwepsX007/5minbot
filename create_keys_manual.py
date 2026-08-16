import os
import time
import json
import requests
from dotenv import load_dotenv
from eth_account import Account
from eth_account.messages import encode_defunct

load_dotenv()

pk = (os.getenv("POLY_PRIVATE_KEY") or "").strip()
if not pk:
    raise SystemExit("POLY_PRIVATE_KEY is not set")
if not pk.startswith("0x"):
    pk = "0x" + pk

HOST = "https://clob.polymarket.com"
CHAIN_ID = 137  # Polygon

account = Account.from_key(pk)
address = account.address

print(f"Address: {address}")
print(f"Host: {HOST}")

# ===== L1 подпись =====
def build_l1_headers(method, request_path, body=""):
    timestamp = str(int(time.time() * 1000))

    message = timestamp + method + request_path + (body or "")

    msg = encode_defunct(text=message)
    signed = account.sign_message(msg)
    signature = signed.signature.hex()
    if not signature.startswith("0x"):
        signature = "0x" + signature

    return {
        "POLY_ADDRESS":   address,
        "POLY_SIGNATURE": signature,
        "POLY_TIMESTAMP": timestamp,
        "POLY_NONCE":     "0",
        "Content-Type":   "application/json",
    }

# ===== 1. Пробуем derive-api-key =====
print("\n1. Derive API key...")
try:
    headers = build_l1_headers("GET", "/auth/derive-api-key")
    print("Signed headers prepared (redacted).")

    r = requests.get(
        f"{HOST}/auth/derive-api-key",
        headers=headers,
        params={"nonce": "0"},
        timeout=10,
    )
    print(f"Status: {r.status_code}")
    print(f"Response: {r.text}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

# ===== 2. Пробуем create-api-key =====
print("\n2. Create API key...")
try:
    body = json.dumps({"nonce": "0"})
    headers = build_l1_headers("POST", "/auth/api-key", body)

    r = requests.post(
        f"{HOST}/auth/api-key",
        headers=headers,
        data=body,
        timeout=10,
    )
    print(f"Status: {r.status_code}")
    print(f"Response: {r.text}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

# ===== 3. Проверяем существующие ключи =====
print("\n3. Get existing API keys...")
try:
    headers = build_l1_headers("GET", "/auth/api-keys")

    r = requests.get(
        f"{HOST}/auth/api-keys",
        headers=headers,
        timeout=10,
    )
    print(f"Status: {r.status_code}")
    print(f"Response: {r.text}")
except Exception as e:
    print(f"Error: {e}")
