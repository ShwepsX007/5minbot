import json
import logging
import os
import time
import math
import tempfile
import requests
from typing import Optional

from eth_account import Account
from eth_account.messages import encode_defunct

import config as cfg

log = logging.getLogger("trading")

HOST = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"

_client = None


# =========================================================
# HELPERS
# =========================================================

def _get_env(key: str, default="") -> str:
    return os.environ.get(key) or getattr(cfg, key, default) or default


def _get_int_env(key: str, default=0) -> int:
    try:
        return int(_get_env(key, str(default)))
    except:
        return default


def _normalize_pk(pk: str) -> str:
    pk = (pk or "").strip()
    if pk and not pk.startswith("0x"):
        pk = "0x" + pk
    return pk


def _is_valid_eth_address(addr: str) -> bool:
    return isinstance(addr, str) and addr.startswith("0x") and len(addr) == 42


def _extract_error_text(err) -> str:
    try:
        return str(err)
    except:
        return repr(err)


def _build_creds():
    api_key = _get_env("POLY_API_KEY").strip()
    api_sec = _get_env("POLY_API_SECRET").strip()
    api_pass = _get_env("POLY_API_PASSPHRASE").strip()

    if not (api_key and api_sec and api_pass):
        return None

    try:
        from py_clob_client_v2.clob_types import ApiCreds
        creds = ApiCreds(
            api_key=api_key,
            api_secret=api_sec,
            api_passphrase=api_pass,
        )
        log.info("✅ API creds загружены")
        return creds
    except Exception as e:
        log.warning(f"⚠️ Не удалось создать ApiCreds: {e}")
        return None


def _build_client(signature_type: int):
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.constants import POLYGON

    pk = _normalize_pk(_get_env("POLY_PRIVATE_KEY"))
    funder = _get_env("POLY_FUNDER").strip()
    creds = _build_creds()

    if not pk:
        raise Exception("POLY_PRIVATE_KEY пустой")

    kwargs = {
        "host": HOST,
        "chain_id": POLYGON,
        "key": pk,
        "creds": creds,
        "signature_type": signature_type,
    }

    if signature_type in (1, 2, 3) and funder:
        kwargs["funder"] = funder

    client = ClobClient(**kwargs)
    return client


def _make_balance_params():
    try:
        from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
        return BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    except Exception as e:
        log.warning(f"⚠️ Не удалось создать BalanceAllowanceParams: {e}")
        return None


def _get_balance_allowance_safe(client):
    params = _make_balance_params()
    if params is not None:
        return client.get_balance_allowance(params)

    try:
        return client.get_balance_allowance()
    except TypeError:
        return client.get_balance_allowance({})


def _update_balance_allowance_safe(client):
    params = _make_balance_params()
    if params is not None:
        return client.update_balance_allowance(params)

    try:
        return client.update_balance_allowance()
    except TypeError:
        return client.update_balance_allowance({})


def _object_to_dict(obj):
    if obj is None:
        return {}

    if isinstance(obj, dict):
        return dict(obj)

    try:
        if hasattr(obj, "__dict__"):
            d = dict(vars(obj))
            if d:
                return d
    except:
        pass

    result = {}
    for key in [
        "id", "orderID", "order_id", "orderId",
        "side", "price",
        "original_size", "size", "remaining_size", "initial_size",
        "status", "asset_id", "token_id"
    ]:
        try:
            val = getattr(obj, key, None)
            if val is not None:
                result[key] = val
        except:
            pass
    return result


def _extract_order_id(order) -> str:
    d = _object_to_dict(order)
    for k in ("id", "orderID", "order_id", "orderId"):
        v = d.get(k)
        if v:
            return str(v)
    return ""


def _normalize_open_order(order) -> dict:
    d = _object_to_dict(order)
    oid = _extract_order_id(d)
    side = d.get("side") or d.get("order_side") or d.get("orderSide") or "?"
    original_size = (
        d.get("original_size")
        or d.get("size")
        or d.get("initial_size")
        or d.get("remaining_size")
        or d.get("amount")
        or "?"
    )

    return {
        **d,
        "id": oid or "?",
        "side": side,
        "original_size": original_size,
    }


def _is_cancel_success(res, order_id: str) -> bool:
    if res is None:
        return False

    if isinstance(res, dict):
        if res.get("error"):
            return False

        if res.get("success") is True:
            return True

        for key in ("canceled", "cancelled", "cancelledOrderIds", "canceled_order_ids", "canceledOrderIds"):
            val = res.get(key)
            if isinstance(val, list) and str(order_id) in [str(x) for x in val]:
                return True

        # Частый случай: dict без error уже означает успех
        return True

    if isinstance(res, list):
        return any(str(x) == str(order_id) for x in res) or len(res) > 0

    if isinstance(res, str):
        low = res.lower()
        if "error" in low or "fail" in low:
            return False
        if "cancel" in low:
            return True

    return bool(res)


# =========================================================
# INIT
# =========================================================

def init_trading() -> bool:
    global _client
    _client = None

    pk = _normalize_pk(_get_env("POLY_PRIVATE_KEY"))
    funder = _get_env("POLY_FUNDER").strip()
    configured_sig_type = _get_int_env("POLY_SIGNATURE_TYPE", 1)

    log.info(
        f"🔧 init_trading | sig_type={configured_sig_type} | "
        f"funder={funder} | api_key={'yes' if _get_env('POLY_API_KEY') else 'no'}"
    )

    if not pk:
        log.error("❌ POLY_PRIVATE_KEY пустой")
        return False

    try:
        eoa_address = Account.from_key(pk).address
        log.info(f"✅ EOA адрес: {eoa_address}")
    except Exception as e:
        log.error(f"❌ Неверный приватный ключ: {e}")
        return False

    if funder and not _is_valid_eth_address(funder):
        log.error(f"❌ POLY_FUNDER некорректен: {funder}")
        return False

    sig_types_to_try = []
    for st in [configured_sig_type, 1, 2, 3, 0]:
        if st not in sig_types_to_try:
            sig_types_to_try.append(st)

    for st in sig_types_to_try:
        try:
            if st in (1, 2, 3) and not funder:
                log.warning(f"⚠️ Пропускаю sig_type={st}, потому что POLY_FUNDER пустой")
                continue

            log.info(f"🔄 Пробую signature_type={st}...")
            client = _build_client(st)

            try:
                ok = client.get_ok()
                log.info(f"✅ CLOB ping OK sig_type={st}: {ok}")
            except Exception as e:
                log.warning(f"⚠️ CLOB ping failed sig_type={st}: {e}")

            try:
                addr = client.get_address()
                log.info(f"✅ Client address sig_type={st}: {addr}")
            except Exception as e:
                log.warning(f"⚠️ get_address failed sig_type={st}: {e}")

            try:
                bal = _get_balance_allowance_safe(client)
                log.info(f"✅ Баланс sig_type={st}: {bal}")
                _client = client
                update_env_and_config({"POLY_SIGNATURE_TYPE": str(st)})
                log.info(f"✅ Клиент инициализирован sig_type={st}")
                return True
            except Exception as e:
                err = _extract_error_text(e)
                log.warning(f"⚠️ Баланс sig_type={st}: {err}")

                lowered = err.lower()
                if (
                    "unauthorized" not in lowered
                    and "forbidden" not in lowered
                    and "maker address not allowed" not in lowered
                ):
                    _client = client
                    update_env_and_config({"POLY_SIGNATURE_TYPE": str(st)})
                    log.info(f"✅ Клиент принят sig_type={st}")
                    return True

        except Exception as e:
            log.error(f"❌ sig_type={st} failed: {e}")
            import traceback
            log.error(traceback.format_exc())
            continue

    log.error("❌ Ни один sig_type не сработал")
    return False


def is_ready() -> bool:
    return _client is not None


def get_wallet_address() -> Optional[str]:
    try:
        funder = _get_env("POLY_FUNDER").strip()
        sig_type = _get_int_env("POLY_SIGNATURE_TYPE", 1)
        pk = _normalize_pk(_get_env("POLY_PRIVATE_KEY"))

        if sig_type in (1, 2, 3) and funder:
            return funder

        if pk:
            return Account.from_key(pk).address

        return None
    except Exception as e:
        log.error(f"get_wallet_address error: {e}")
        return None


def get_eoa_address() -> Optional[str]:
    try:
        pk = _normalize_pk(_get_env("POLY_PRIVATE_KEY"))
        if not pk:
            return None
        return Account.from_key(pk).address
    except:
        return None


# =========================================================
# MARKET INFO
# =========================================================

def get_market_info(token_id) -> Optional[dict]:
    try:
        r = requests.get(
            f"{GAMMA}/markets",
            params={"clob_token_ids": str(token_id)},
            timeout=10
        )
        if r.status_code != 200:
            return None

        data = r.json()
        if isinstance(data, list) and data:
            m = data[0]
            return {
                "neg_risk": m.get("negRisk", False),
                "accepting_orders": m.get("acceptingOrders", False),
                "closed": m.get("closed", True),
                "min_size": float(m.get("orderMinSize", 5)),
            }
        return None
    except Exception as e:
        log.warning(f"get_market_info error: {e}")
        return None


def get_event_markets(slug: str) -> Optional[dict]:
    def _as_list(val):
        if isinstance(val, list):
            return val
        if isinstance(val, str):
            try:
                parsed = json.loads(val)
                return parsed if isinstance(parsed, list) else []
            except:
                return []
        return []

    def _to_cents(val):
        try:
            x = float(val)
            if x <= 1:
                x *= 100
            return max(0, min(100, round(x)))
        except:
            return 0

    try:
        r = requests.get(f"{GAMMA}/events/slug/{slug}", timeout=15)
        if r.status_code != 200:
            log.warning(f"get_event_markets bad status {r.status_code} for slug={slug}")
            return None

        data = r.json()
        if isinstance(data, list):
            data = data[0] if data else {}

        if not isinstance(data, dict):
            log.warning(f"get_event_markets invalid response type for slug={slug}")
            return None

        markets = []

        for m in data.get("markets", []) or []:
            tids = _as_list(m.get("clobTokenIds"))
            prices = _as_list(m.get("outcomePrices"))
            outcomes = _as_list(m.get("outcomes"))

            if len(tids) < 2:
                continue

            yes_idx, no_idx = 0, 1

            if len(outcomes) >= 2:
                lowered = [str(x).strip().lower() for x in outcomes]
                try:
                    yes_idx = lowered.index("yes")
                    no_idx = lowered.index("no")
                except:
                    yes_idx, no_idx = 0, 1

            if yes_idx >= len(tids) or no_idx >= len(tids):
                yes_idx, no_idx = 0, 1

            price_yes = _to_cents(prices[yes_idx]) if len(prices) > yes_idx else 0
            price_no = _to_cents(prices[no_idx]) if len(prices) > no_idx else 0

            active = m.get("active")
            if active is None:
                active = not bool(m.get("closed", False))

            markets.append({
                "question": m.get("groupItemTitle") or m.get("question") or "Без названия",
                "token_yes": str(tids[yes_idx]),
                "token_no": str(tids[no_idx]),
                "price_yes": price_yes,
                "price_no": price_no,
                "active": bool(active),
                "neg_risk": bool(m.get("negRisk", False)),
                "accepting_orders": bool(m.get("acceptingOrders", active)),
            })

        if not markets:
            log.warning(f"get_event_markets: empty markets for slug={slug}")

        return {
            "title": data.get("title") or slug,
            "markets": markets
        }

    except Exception as e:
        log.warning(f"get_event_markets error for slug={slug}: {e}")
        return None


# =========================================================
# BALANCE
# =========================================================

def get_balance() -> Optional[float]:
    try:
        if _client is None:
            return None

        data = _get_balance_allowance_safe(_client)

        if isinstance(data, dict):
            bal = data.get("balance")
            if bal is None:
                bal = data.get("collateral_token_balance", 0)
            return round(float(bal) / 1_000_000, 2)

        return None

    except Exception as e:
        log.warning(f"get_balance error: {e}")
        return None


# =========================================================
# ORDER PLACEMENT CORE
# =========================================================

def _post_order_with_client(client, token_id, side: str, price: float, size: float):
    from py_clob_client_v2.clob_types import OrderArgsV2

    order_args = OrderArgsV2(
        token_id=str(token_id),
        price=price,
        size=size,
        side=side,
    )

    try:
        result = client.create_and_post_order(order_args)
        log.info(f"✅ Order placed (create_and_post_order): {result}")
        return result
    except Exception as e:
        err1 = e
        log.warning(f"create_and_post_order failed: {e}")

    try:
        order = client.create_order(order_args)
        log.info(f"create_order result type: {type(order)}")
        try:
            maker = getattr(order, "maker", None)
            if maker:
                log.info(f"order.maker = {maker}")
        except:
            pass

        result = client.post_order(order)
        log.info(f"✅ Order placed (create_order + post_order): {result}")
        return result
    except Exception as e:
        err2 = e
        log.warning(f"create_order + post_order failed: {e}")

    raise err2 if 'err2' in locals() else err1


# =========================================================
# TRADING
# =========================================================

def place_order(token_id, side: str, price: float, size: float) -> dict:
    global _client

    try:
        if _client is None:
            return {"error": "Trading client not initialized"}

        side = "BUY" if side.upper() == "BUY" else "SELL"
        price = float(price)
        size = float(size)
        if not math.isfinite(price) or not 0.01 <= price <= 100:
            return {"error": "Price must be between 0.01 and 1.00"}
        if not math.isfinite(size) or size <= 0:
            return {"error": "Size must be a positive finite number"}

        if price > 1:
            price = round(price / 100, 4)

        market_info = get_market_info(token_id)
        if market_info:
            if not market_info["accepting_orders"]:
                return {"error": "Market is closed or resolved"}
            if size < market_info["min_size"]:
                size = market_info["min_size"]
                log.info(f"Size adjusted to minimum: {size}")

        log.info(f"Placing order: {side} {size}@{price} | token={token_id}")

        try:
            _update_balance_allowance_safe(_client)
            log.info("✅ Balance allowance updated")
        except Exception as e:
            log.warning(f"⚠️ Could not update allowance: {e}")

        current_sig = _get_int_env("POLY_SIGNATURE_TYPE", 1)
        sig_candidates = []
        for st in [current_sig, 1, 2, 3, 0]:
            if st not in sig_candidates:
                sig_candidates.append(st)

        last_error = None

        for st in sig_candidates:
            try:
                log.info(f"🔄 Пробую отправить ордер через sig_type={st}...")

                client = _build_client(st)

                try:
                    _update_balance_allowance_safe(client)
                except Exception as e:
                    log.warning(f"allowance update sig_type={st} failed: {e}")

                result = _post_order_with_client(client, token_id, side, price, size)

                _client = client
                update_env_and_config({"POLY_SIGNATURE_TYPE": str(st)})
                log.info(f"✅ Рабочий sig_type для ордера: {st}")
                return result

            except Exception as e:
                last_error = e
                err = _extract_error_text(e)
                log.warning(f"sig_type={st} order failed: {err}")

                if "maker address not allowed" in err.lower():
                    continue
                continue

        return {"error": str(last_error)}

    except Exception as e:
        log.error(f"place_order error: {e}")
        import traceback
        log.error(traceback.format_exc())
        return {"error": str(e)}


# =========================================================
# ORDERS
# =========================================================

def get_open_orders() -> list:
    global _client

    def _normalize_collection(raw):
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            for key in ("data", "orders", "open_orders", "results"):
                if key in raw:
                    val = raw[key]
                    if isinstance(val, list):
                        return val
                    if isinstance(val, dict):
                        return [val]
            return []
        return []

    try:
        if _client is None:
            return []

        try:
            raw = _client.get_open_orders()
        except Exception as e:
            log.warning(f"get_open_orders current client failed: {e}")
            current_sig = _get_int_env("POLY_SIGNATURE_TYPE", 1)
            _client = _build_client(current_sig)
            raw = _client.get_open_orders()

        items = _normalize_collection(raw)
        normalized = [_normalize_open_order(o) for o in items]
        normalized = [o for o in normalized if o.get("id")]
        return normalized

    except Exception as e:
        log.error(f"get_open_orders error: {e}")
        return []


def cancel_order(order_id: str) -> dict:
    global _client

    try:
        order_id = str(order_id or "").strip()
        if not order_id or order_id == "?":
            return {"error": "Invalid order id"}

        current_sig = _get_int_env("POLY_SIGNATURE_TYPE", 1)
        sig_candidates = []
        for st in [current_sig, 1, 2, 3, 0]:
            if st not in sig_candidates:
                sig_candidates.append(st)

        last_error = None

        for st in sig_candidates:
            try:
                log.info(f"🔄 Пробую отменить ордер {order_id} через sig_type={st}...")
                client = _build_client(st)

                attempts = []

                if hasattr(client, "cancel_order"):
                    attempts.append(("cancel_order(order_id)", lambda: client.cancel_order(order_id)))

                if hasattr(client, "cancel"):
                    attempts.append(("cancel(order_id)", lambda: client.cancel(order_id)))

                if hasattr(client, "cancel_orders"):
                    attempts.append(("cancel_orders([order_id])", lambda: client.cancel_orders([order_id])))
                    attempts.append(("cancel_orders({'order_ids':[order_id]})", lambda: client.cancel_orders({"order_ids": [order_id]})))
                    attempts.append(("cancel_orders(order_ids=[order_id])", lambda: client.cancel_orders(order_ids=[order_id])))

                for name, fn in attempts:
                    try:
                        res = fn()
                        log.info(f"cancel attempt {name} => {res}")

                        if _is_cancel_success(res, order_id):
                            _client = client
                            update_env_and_config({"POLY_SIGNATURE_TYPE": str(st)})
                            return {"success": True, "order_id": order_id, "result": res}

                    except Exception as e:
                        last_error = e
                        log.warning(f"{name} failed on sig_type={st}: {e}")

            except Exception as e:
                last_error = e
                log.warning(f"cancel build_client failed on sig_type={st}: {e}")

        return {"error": str(last_error) if last_error else "Cancel failed"}

    except Exception as e:
        return {"error": str(e)}


def cancel_all() -> dict:
    try:
        if _client is None:
            return {"error": "Not initialized"}
        return _client.cancel_all()
    except Exception as e:
        return {"error": str(e)}


# =========================================================
# CONFIG UPDATE
# =========================================================

def update_env_and_config(updates: dict) -> bool:
    try:
        for key, value in updates.items():
            setattr(cfg, key, int(value) if key == "POLY_SIGNATURE_TYPE" else str(value))
            os.environ[key] = str(value)

        env_path = os.path.join(cfg.BASE_DIR, ".env")
        env_dict = {}

        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        k, v = line.split("=", 1)
                        env_dict[k.strip()] = v.strip()

        for k, v in updates.items():
            env_dict[k] = str(v)

        fd, temp_path = tempfile.mkstemp(prefix=".env.", dir=cfg.BASE_DIR, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                for k, v in env_dict.items():
                    f.write(f"{k}={v}\n")
            os.chmod(temp_path, 0o600)
            os.replace(temp_path, env_path)
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

        log.info(f"✅ Config updated: {list(updates.keys())}")
        return True

    except Exception as e:
        log.error(f"update_env_and_config error: {e}")
        return False


# =========================================================
# AUTO GENERATE API KEYS
# =========================================================

def auto_generate_polymarket_keys(private_key: str) -> Optional[dict]:
    if not private_key.startswith("0x"):
        private_key = "0x" + private_key

    try:
        account = Account.from_key(private_key)
        address = account.address
        log.info(f"🔑 Генерирую API ключи для EOA: {address}")

        def build_headers(method: str, path: str, body: str = "") -> dict:
            timestamp = str(int(time.time()))
            nonce = "0"
            message = timestamp + method.upper() + path + (body or "")
            msg = encode_defunct(text=message)
            signed = account.sign_message(msg)
            sig = signed.signature.hex()
            if not sig.startswith("0x"):
                sig = "0x" + sig
            log.info(f"L1 header: addr={address}, ts={timestamp}, method={method}, path={path}")
            return {
                "POLY_ADDRESS": address,
                "POLY_SIGNATURE": sig,
                "POLY_TIMESTAMP": timestamp,
                "POLY_NONCE": nonce,
                "Content-Type": "application/json",
            }

        def extract_creds(resp) -> Optional[dict]:
            if resp is None:
                return None
            if isinstance(resp, dict):
                k = resp.get("apiKey") or resp.get("api_key")
                s = resp.get("secret") or resp.get("api_secret")
                p = resp.get("passphrase") or resp.get("api_passphrase")
            else:
                k = getattr(resp, "api_key", None) or getattr(resp, "apiKey", None)
                s = getattr(resp, "api_secret", None) or getattr(resp, "secret", None)
                p = getattr(resp, "api_passphrase", None) or getattr(resp, "passphrase", None)

            if k and s and p:
                return {
                    "POLY_API_KEY": str(k),
                    "POLY_API_SECRET": str(s),
                    "POLY_API_PASSPHRASE": str(p),
                }
            return None

        body = json.dumps({"nonce": 0})
        headers = build_headers("POST", "/auth/api-key", body)
        r = requests.post(f"{HOST}/auth/api-key", headers=headers, data=body, timeout=15)
        log.info(f"POST /auth/api-key → {r.status_code}: {r.text[:400]}")
        if r.status_code in (200, 201):
            result = extract_creds(r.json())
            if result:
                log.info("✅ Способ 1: новые ключи созданы!")
                return result

        headers = build_headers("GET", "/auth/derive-api-key")
        r = requests.get(
            f"{HOST}/auth/derive-api-key",
            headers=headers,
            params={"nonce": 0},
            timeout=15,
        )
        log.info(f"GET /auth/derive-api-key → {r.status_code}: {r.text[:400]}")
        if r.status_code == 200:
            result = extract_creds(r.json())
            if result:
                log.info("✅ Способ 2: существующие ключи получены!")
                return result

        log.info("Пробую способ 3: ClobClient.create_api_key()...")
        try:
            temp_client = _build_client(0)
            resp = temp_client.create_api_key(nonce=0)
            log.info(f"create_api_key → {resp}")
            result = extract_creds(resp)
            if result:
                log.info("✅ Способ 3: ключи через create_api_key()")
                return result
        except Exception as e3:
            log.error(f"Способ 3 failed: {e3}")

        log.info("Пробую способ 4: ClobClient.create_or_derive_api_key()...")
        try:
            temp_client = _build_client(0)
            resp = temp_client.create_or_derive_api_key(nonce=0)
            log.info(f"create_or_derive_api_key → {resp}")
            result = extract_creds(resp)
            if result:
                log.info("✅ Способ 4: ключи через create_or_derive_api_key()")
                return result
        except Exception as e4:
            log.error(f"Способ 4 failed: {e4}")

        log.info("Пробую способ 5: ClobClient.derive_api_key()...")
        try:
            temp_client = _build_client(0)
            resp = temp_client.derive_api_key(nonce=0)
            log.info(f"derive_api_key → {resp}")
            result = extract_creds(resp)
            if result:
                log.info("✅ Способ 5: ключи через derive_api_key()")
                return result
        except Exception as e5:
            log.error(f"Способ 5 failed: {e5}")

        log.error("❌ Все способы не сработали")
        return None

    except Exception as e:
        log.error(f"auto_generate_polymarket_keys fatal: {e}")
        import traceback
        log.error(traceback.format_exc())
        return None
