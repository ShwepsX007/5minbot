"""
Минимальный пример: рыночный вход в рынок Polymarket ордерами
FAK (Fill-and-Kill — исполнить максимум, остаток отменить)
и FOK (Fill-or-Kill — всё или ничего). Без отложенных лимитных GTC.

Переносится в любой бот: нужны только py_clob_client_v2 (форк
официального py-clob-client, стоящий на сервере рядом с этим ботом),
eth_account и requests.

Переменные окружения (.env), как в config.py этого бота:
  POLY_PRIVATE_KEY      — приватный ключ торгующего кошелька (0x...)
  POLY_API_KEY          ┐
  POLY_API_SECRET       ├ L2-ключи CLOB (генерируются create_keys_manual.py
  POLY_API_PASSPHRASE   ┘ или auto_generate_polymarket_keys())
  POLY_FUNDER           — прокси-адрес кошелька с сайта (для sig_type 1/2/3)
  POLY_SIGNATURE_TYPE   — 0: свой EOA; 1/2: email/магия сайта; 3: бот-кошелёк

Запуск:  python3 example_market_order.py
"""

import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

CLOB_HOST = "https://clob.polymarket.com"
GAMMA_HOST = "https://gamma-api.polymarket.com"
CHAIN_ID = 137  # Polygon


# ============================================================
# 1. ПОДКЛЮЧЕНИЕ К CLOB
# ============================================================
def build_client():
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import ApiCreds

    pk = os.environ["POLY_PRIVATE_KEY"].strip()
    if not pk.startswith("0x"):
        pk = "0x" + pk

    sig_type = int(os.environ.get("POLY_SIGNATURE_TYPE", "1"))
    kwargs = {
        "host": CLOB_HOST,
        "chain_id": CHAIN_ID,
        "key": pk,
        "creds": ApiCreds(
            api_key=os.environ["POLY_API_KEY"].strip(),
            api_secret=os.environ["POLY_API_SECRET"].strip(),
            api_passphrase=os.environ["POLY_API_PASSPHRASE"].strip(),
        ),
        "signature_type": sig_type,
    }
    funder = (os.environ.get("POLY_FUNDER") or "").strip()
    if sig_type in (1, 2, 3) and funder:
        kwargs["funder"] = funder  # кошелёк, где лежит USDC (прокси с сайта)

    client = ClobClient(**kwargs)

    # ОБЯЗАТЕЛЬНО перед первым ордером: обновить allowance USDC.
    # Без этого CLOB отвечает "not enough balance / allowance".
    client.update_balance_allowance()
    return client


# ============================================================
# 2. НАЙТИ РЫНОК (Gamma API): slug события -> token_id исхода
# ============================================================
def find_token_by_slug(slug: str, want_outcome: str = "yes") -> tuple[str, float]:
    """Возвращает (token_id, цену в долях 0..1) для исхода 'yes'/'no'.

    Для погодных рынков с многими исходами (бакеты температуры) ищи
    событие по slug и перебирайт markets[] — у каждого свой token_id.
    """
    r = requests.get(f"{GAMMA_HOST}/events/slug/{slug}", timeout=15)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        data = data[0]

    for m in data.get("markets", []):
        tids = m.get("clobTokenIds") or "[]"
        if isinstance(tids, str):
            import json
            tids = json.loads(tids)
        prices = m.get("outcomePrices") or "[]"
        if isinstance(prices, str):
            import json
            prices = json.loads(prices)
        outcomes = m.get("outcomes") or "[]"
        if isinstance(outcomes, str):
            import json
            outcomes = json.loads(outcomes)

        lowered = [str(o).strip().lower() for o in outcomes]
        idx = lowered.index(want_outcome.lower()) if want_outcome.lower() in lowered else 0
        token_id = tids[idx]
        price = float(prices[idx]) if prices else 0.5
        return token_id, price

    raise SystemExit(f"рынок {slug} не найден")


# ============================================================
# 3. РЫНОЧНЫЙ ОРДЕР FAK / FOK — суть переноса
# ============================================================
def market_order(client, token_id: str, side: str, amount: float,
                 order_type: str = "FAK") -> dict:
    """side='BUY'  -> amount = сумма в ДОЛЛАРАХ (минимум $1);
       side='SELL' -> amount = количество ДОЛЕЙ (сумма тоже >= $1).

       order_type:
         'FAK' — исполнить максимум по лучшим ценам, остаток отменить
                 (лучше для тонких стаканов: погода, свежие окна);
         'FOK' — всё или ничего: не смог исполнить весь объём — отмена.
    """
    from py_clob_client_v2.clob_types import MarketOrderArgsV2, OrderType

    ot = OrderType.FOK if order_type.upper() == "FOK" else OrderType.FAK
    args = MarketOrderArgsV2(
        token_id=str(token_id),
        amount=float(amount),
        side=side.upper(),
        order_type=ot,
    )
    # ВАЖНО: это НЕ create_and_post_order(OrderArgs(price, size)) —
    # тот шлёт лимитный GTC («отложник»). Рынок = MarketOrderArgs
    # + create_and_post_market_order + явный order_type.
    return client.create_and_post_market_order(args, order_type=ot)


# ============================================================
# 4. ПРОВЕРКА: исполнен ли ордер на самом деле
# ============================================================
def order_result(res) -> tuple[bool, str, dict]:
    """CLOB отклоняет ордера через success/errorMsg, а не через 'error' —
    не проверив эти ключи, легко записать фантомную позицию."""
    if not isinstance(res, dict):
        return False, "пустой ответ", {}
    if res.get("success") is False or str(res.get("errorMsg") or "").strip():
        return False, str(res.get("errorMsg") or "success=false"), {}
    status = str(res.get("status") or "").lower()
    if status in ("unmatched", "cancelled", "canceled", "expired", "rejected"):
        return False, f"не исполнен (статус {status})", {}

    # takingAmount — что получили (BUY: доли), makingAmount — что отдали (BUY: USDC)
    try:
        shares = float(res.get("takingAmount") or 0) or None
    except ValueError:
        shares = None
    try:
        cost = float(res.get("makingAmount") or 0) or None
    except ValueError:
        cost = None
    return True, "", {"shares": shares, "cost": cost, "status": status or "?"}


# ============================================================
# ДЕМО
# ============================================================
if __name__ == "__main__":
    client = build_client()

    # любой слуг события, например погодный или крипто:
    token_id, price = find_token_by_slug(
        "will-it-rain-in-new-york-city-on-september-3", "yes")
    print(f"token={token_id[:16]}… цена {price:.2f}")

    usd = 5.0  # хотим купить на $5
    res = market_order(client, token_id, "BUY", usd, order_type="FAK")
    print("ответ CLOB:", res)

    ok, why, fill = order_result(res)
    if ok:
        shares = fill.get("shares") or 0
        cost = fill.get("cost") or usd
        avg = cost / shares if shares else 0
        print(f"✅ куплено {shares} долей, потрачено ${cost:.2f}, средняя {avg:.2f}$")
    else:
        print(f"❌ ордер не исполнен: {why}")
