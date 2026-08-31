"""Поток ордеров (Order Flow) с Binance: CVD и дивергенции.

Зачем: каскад ликвидаций сам по себе не отличает разворот от безоткатного
пампа/дампа. Отличает — КТО двигает цену:

  • цену тащат агрессивные рыночные покупки (CVD растёт вместе с ценой)
    → в рынок заходят новые деньги, откат маловероятен, контр-трейд опасен;
  • цена лезет вверх, а CVD стоит или падает → покупателей поглощают
    крупные лимитные продажи (абсорбция), это классическая дивергенция
    и признак скорого разворота — наш сценарий.

Источник: публичный стрим Binance USDⓈ-M `<symbol>@aggTrade` (ключи не
нужны). В каждой сделке поле "m" — был ли покупатель мейкером:
    m = false → агрессор ПОКУПАЛ  (taker buy)  → +объём
    m = true  → агрессор ПРОДАВАЛ (taker sell) → −объём
Сумма этих знаковых объёмов и есть CVD (Cumulative Volume Delta).
"""

import asyncio
import json
import logging
import time

import aiohttp

log = logging.getLogger("bot.orderflow")

BINANCE_STREAM_URLS = (
    "wss://fstream.binance.com/market/stream?streams=",
    "wss://fstream.binance.com/stream?streams=",
)

# Сколько секунд сделок держим в памяти.
_TRADES_TTL = 900
_MAX_TRADES_PER_SYMBOL = 20000

# symbol ("BTC_USDT") -> список (ts, signed_usd, usd, price)
_trades: dict = {}
_lock = asyncio.Lock()

_desired_symbols: set = set()
_symbols_version = 0          # растёт при смене списка → переподключение

_status = {
    "connected": False,
    "last_msg_ts": 0.0,
    "trades": 0,
    "symbols": 0,
    "last_error": "",
}


def set_symbols(symbols):
    """Список монет для подписки. При изменении поток переподключается."""
    global _desired_symbols, _symbols_version
    new = set()
    for s in (symbols or []):
        if not s:
            continue
        v = str(s).upper().replace("-", "_")
        if not v.endswith("_USDT") and v.endswith("USDT"):
            v = v[:-4] + "_USDT"
        new.add(v)
    if new != _desired_symbols:
        _desired_symbols = new
        _symbols_version += 1
        _status["symbols"] = len(new)


def _stream_name(symbol: str) -> str:
    return symbol.replace("_", "").lower() + "@aggTrade"


def _norm_symbol(raw: str) -> str:
    s = str(raw or "").upper()
    if s.endswith("USDT") and "_" not in s:
        s = s[:-4] + "_USDT"
    return s


def _store(symbol: str, ts: float, price: float, qty: float, is_buyer_maker: bool):
    usd = price * qty
    signed = -usd if is_buyer_maker else usd     # m=true → продажа агрессором
    arr = _trades.setdefault(symbol, [])
    arr.append((ts, signed, usd, price))

    if len(arr) > _MAX_TRADES_PER_SYMBOL:
        cutoff = time.time() - _TRADES_TTL
        _trades[symbol] = [t for t in arr if t[0] >= cutoff][-_MAX_TRADES_PER_SYMBOL:]


def _parse_agg_trade(data):
    """Возвращает (symbol, ts_sec, price, qty, is_buyer_maker) или None."""
    if not isinstance(data, dict):
        return None
    # combined stream оборачивает событие в {"stream": ..., "data": {...}}
    if "data" in data and "e" not in data:
        data = data.get("data") or {}
    if data.get("e") != "aggTrade":
        return None
    try:
        sym = _norm_symbol(data.get("s"))
        price = float(data.get("p") or 0)
        qty = float(data.get("q") or 0)
    except (TypeError, ValueError):
        return None
    if not sym or price <= 0 or qty <= 0:
        return None
    ts = data.get("T") or data.get("E") or 0
    try:
        ts = float(ts) / 1000.0 if ts else time.time()
    except (TypeError, ValueError):
        ts = time.time()
    return sym, ts, price, qty, bool(data.get("m"))


async def binance_aggtrade_listener():
    """Слушает aggTrade по выбранным монетам и копит поток сделок."""
    url_idx = 0
    while True:
        session = None
        ws = None
        version = _symbols_version
        symbols = sorted(_desired_symbols)

        if not symbols:
            await asyncio.sleep(5)
            continue

        base = BINANCE_STREAM_URLS[url_idx % len(BINANCE_STREAM_URLS)]
        url = base + "/".join(_stream_name(s) for s in symbols)

        try:
            log.info(f"[OrderFlow] Connecting ({len(symbols)} symbols)...")
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None))
            ws = await session.ws_connect(url, heartbeat=20, autoping=True)
            _status["connected"] = True
            _status["last_error"] = ""
            log.info("[OrderFlow] Connected!")

            while True:
                # Список монет поменялся — пересобираем подписку
                if _symbols_version != version:
                    log.info("[OrderFlow] список монет изменился, переподключаюсь")
                    break

                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=30)
                except asyncio.TimeoutError:
                    continue

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except Exception:
                        continue
                    parsed = _parse_agg_trade(data)
                    if not parsed:
                        continue
                    sym, ts, price, qty, maker = parsed
                    async with _lock:
                        _store(sym, ts, price, qty, maker)
                    _status["trades"] += 1
                    _status["last_msg_ts"] = time.time()

                elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                  aiohttp.WSMsgType.ERROR):
                    log.warning("[OrderFlow] Lost")
                    url_idx += 1
                    break

        except asyncio.CancelledError:
            log.info("[OrderFlow] остановлен")
            raise
        except Exception as e:
            _status["last_error"] = str(e)
            log.warning(f"[OrderFlow] err: {e}")
            url_idx += 1
        finally:
            _status["connected"] = False
            try:
                if ws and not ws.closed:
                    await ws.close()
            except Exception:
                pass
            try:
                if session and not session.closed:
                    await session.close()
            except Exception:
                pass

        await asyncio.sleep(3)


def flow_stats(symbol: str, t0: float, t1: float) -> dict | None:
    """Статистика потока за интервал [t0, t1].

    cvd        — знаковый объём в $ (покупки минус продажи);
    imbalance  — cvd / общий объём, от -1 (только продажи) до +1;
    price_from/price_to — первая и последняя цена сделки в интервале.
    """
    arr = _trades.get(symbol)
    if not arr:
        return None

    cvd = 0.0
    buy = 0.0
    sell = 0.0
    n = 0
    price_from = price_to = None
    high = low = None

    for ts, signed, usd, price in arr:
        if ts < t0:
            continue
        if ts > t1:
            break
        cvd += signed
        if signed >= 0:
            buy += usd
        else:
            sell += usd
        n += 1
        if price_from is None:
            price_from = price
        price_to = price
        high = price if high is None else max(high, price)
        low = price if low is None else min(low, price)

    total = buy + sell
    if n == 0 or total <= 0:
        return None

    return {
        "cvd": cvd,
        "buy": buy,
        "sell": sell,
        "total": total,
        "imbalance": cvd / total,
        "trades": n,
        "price_from": price_from,
        "price_to": price_to,
        "high": high,
        "low": low,
    }


def divergence(symbol: str, t0: float, t1: float) -> str | None:
    """Ищет дивергенцию цены и CVD внутри интервала.

    Делим интервал пополам и сравниваем экстремум цены и накопленную
    дельту:

      BEAR_DIV — во второй половине цена сделала более высокий максимум,
                 а CVD при этом ниже, чем в первой: покупателей поглощают
                 лимитными продажами → разворот вниз;
      BULL_DIV — зеркально для низов.

    Возвращает None, если данных мало или дивергенции нет.
    """
    if t1 <= t0:
        return None
    mid = t0 + (t1 - t0) / 2.0
    first = flow_stats(symbol, t0, mid)
    second = flow_stats(symbol, mid, t1)
    if not first or not second:
        return None
    if first["trades"] < 20 or second["trades"] < 20:
        return None

    if second["high"] > first["high"] and second["cvd"] < first["cvd"]:
        return "BEAR_DIV"
    if second["low"] < first["low"] and second["cvd"] > first["cvd"]:
        return "BULL_DIV"
    return None


def have_data(symbol: str) -> bool:
    arr = _trades.get(symbol)
    return bool(arr) and (time.time() - arr[-1][0]) < 120


def cleanup():
    """Подрезает буферы (вызывается стратегией время от времени)."""
    cutoff = time.time() - _TRADES_TTL
    for sym, arr in list(_trades.items()):
        if arr and arr[0][0] < cutoff:
            _trades[sym] = [t for t in arr if t[0] >= cutoff]


def ensure_symbol(symbol: str):
    """Добавляет монету к подписке, НЕ сбрасывая остальные (set_symbols
    вызывается обеими стратегиями по очереди — additive-вариант нужен,
    чтобы трендовая стратегия не выписывала монеты ликвидационной и наоборот)."""
    global _desired_symbols, _symbols_version
    if not symbol:
        return
    v = str(symbol).upper().replace("-", "_")
    if not v.endswith("_USDT") and v.endswith("USDT"):
        v = v[:-4] + "_USDT"
    if v and v not in _desired_symbols:
        _desired_symbols = _desired_symbols | {v}
        _symbols_version += 1
        _status["symbols"] = len(_desired_symbols)


def status() -> dict:
    last = _status.get("last_msg_ts") or 0
    return {
        "connected": bool(_status.get("connected")),
        "trades": int(_status.get("trades", 0)),
        "symbols": int(_status.get("symbols", 0)),
        "age_sec": (time.time() - last) if last else None,
        "last_error": _status.get("last_error", ""),
    }
