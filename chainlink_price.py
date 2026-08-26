"""Цена BTC/ETH/… из того же источника, по которому считает Polymarket.

Polymarket Up/Down рассчитывается НЕ по споту биржи, а по Chainlink
Data Streams (страница data.chain.link/streams/btc-usd). С 7 августа 2026
это TWAP-поток: у 5-минутных рынков окно усреднения 30 секунд, у 15m и 4h —
60 секунд. Именно поэтому спот Gate.io иногда показывает дожи там, где на
Polymarket две явные красные свечи.

Забирать данные можно двумя путями:
  • напрямую у Chainlink Data Streams — нужны API-ключи (client id/secret);
  • через Polymarket RTDS — публичный WebSocket, ключи не нужны.

Здесь используется второй путь:
    wss://ws-live-data.polymarket.com
    топики crypto_prices_twap_thirty (30с) и crypto_prices_twap_sixty (60с)

Модуль копит тики в памяти и умеет собирать из них свечу конкретного окна
Polymarket: open — первый тик на границе окна или позже, close — последний
тик до конца окна (или текущий, если окно ещё идёт).
"""

import asyncio
import json
import logging
import time

import aiohttp

log = logging.getLogger("bot.chainlink")

RTDS_WS = "wss://ws-live-data.polymarket.com"

TOPIC_BY_WINDOW = {
    30: "crypto_prices_twap_thirty",
    60: "crypto_prices_twap_sixty",
}

# Окно усреднения TWAP, которое Polymarket применяет к своим рынкам.
TWAP_WINDOW_BY_TF = {
    "5m": 30,
    "15m": 60,
    "1h": 60,
}

# Сколько секунд тиков держим в памяти (хватает на несколько окон).
_TICKS_TTL = 3600
# Предохранитель от разрастания памяти.
_MAX_TICKS_PER_KEY = 8000

# key = (symbol_norm, window_seconds) -> список (ts_sec, price), по возрастанию
_ticks: dict = {}
_lock = asyncio.Lock()

_status = {
    "connected": False,
    "last_msg_ts": 0.0,
    "updates": 0,
    "last_error": "",
}


def to_rtds_symbol(symbol: str) -> str:
    """BTC_USDT -> btc/usd (RTDS отдаёт пары к доллару в нижнем регистре)."""
    if not symbol:
        return ""
    base = str(symbol).upper().split("_")[0].replace("-", "")
    for suffix in ("USDT", "USDC", "USD"):
        if base.endswith(suffix) and len(base) > len(suffix):
            base = base[: -len(suffix)]
            break
    return f"{base.lower()}/usd"


def twap_window_for(timeframe: str) -> int:
    return TWAP_WINDOW_BY_TF.get(timeframe, 30)


def _store(symbol_norm: str, window_s: int, ts: float, price: float):
    key = (symbol_norm, int(window_s))
    arr = _ticks.setdefault(key, [])
    # Тики приходят по порядку, но на всякий случай не ломаем сортировку.
    if arr and ts < arr[-1][0]:
        arr.append((ts, price))
        arr.sort(key=lambda x: x[0])
    else:
        arr.append((ts, price))

    cutoff = time.time() - _TICKS_TTL
    if len(arr) > _MAX_TICKS_PER_KEY or (arr and arr[0][0] < cutoff):
        _ticks[key] = [t for t in arr if t[0] >= cutoff][-_MAX_TICKS_PER_KEY:]


def _parse_update(data) -> list:
    """Достаёт из сообщения RTDS список (symbol, window_s, ts_sec, price)."""
    out = []
    if not isinstance(data, dict):
        return out

    topic = str(data.get("topic") or "")
    window_s = None
    if "thirty" in topic:
        window_s = 30
    elif "sixty" in topic:
        window_s = 60

    payloads = data.get("payload")
    if isinstance(payloads, dict):
        payloads = [payloads]
    if not isinstance(payloads, list):
        return out

    for p in payloads:
        if not isinstance(p, dict):
            continue
        sym = str(p.get("symbol") or "").lower()
        if not sym:
            continue

        w = p.get("window_s") or p.get("windowSeconds") or window_s
        try:
            w = int(w)
        except (TypeError, ValueError):
            continue

        # full_accuracy_value — точное значение E18, value — для отображения
        price = None
        fav = p.get("full_accuracy_value")
        if fav not in (None, ""):
            try:
                price = float(fav) / 1e18
            except (TypeError, ValueError):
                price = None
        if price is None:
            try:
                price = float(p.get("value"))
            except (TypeError, ValueError):
                continue
        if not price or price <= 0:
            continue

        try:
            ts = float(p.get("timestamp") or 0)
        except (TypeError, ValueError):
            ts = 0
        if ts > 10_000_000_000:      # миллисекунды
            ts = ts / 1000.0
        if not ts:
            ts = time.time()

        out.append((sym, w, ts, price))
    return out


async def rtds_listener(windows=(30, 60)):
    """Слушает публичный RTDS Polymarket и копит TWAP-тики.

    Подписка идёт без фильтра по символу: так один поток закрывает все
    выбранные монеты (в доках прямо рекомендуют фильтровать на своей
    стороне, если нужно несколько пар).
    """
    while True:
        session = None
        ws = None
        try:
            log.info("[Chainlink RTDS] Connecting...")
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None))
            ws = await session.ws_connect(RTDS_WS, heartbeat=None)
            _status["connected"] = True
            _status["last_error"] = ""
            log.info("[Chainlink RTDS] Connected!")

            subs = []
            for w in windows:
                topic = TOPIC_BY_WINDOW.get(int(w))
                if topic:
                    subs.append({"topic": topic, "type": "update"})
            await ws.send_json({"action": "subscribe", "subscriptions": subs})
            log.info(f"[Chainlink RTDS] subscribed: "
                     f"{', '.join(s['topic'] for s in subs)}")

            # RTDS требует текстовый PING каждые 5 секунд
            last_ping = 0.0
            while True:
                now = time.time()
                if now - last_ping >= 5:
                    await ws.send_str("PING")
                    last_ping = now

                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=5)
                except asyncio.TimeoutError:
                    continue

                if msg.type == aiohttp.WSMsgType.TEXT:
                    raw = msg.data
                    if not raw or raw.strip().upper() in ("PONG", "PING"):
                        continue
                    try:
                        data = json.loads(raw)
                    except Exception:
                        continue

                    items = []
                    if isinstance(data, list):
                        for d in data:
                            items.extend(_parse_update(d))
                    else:
                        items.extend(_parse_update(data))

                    if not items:
                        continue

                    async with _lock:
                        for sym, w, ts, price in items:
                            _store(sym, w, ts, price)

                    _status["updates"] += len(items)
                    _status["last_msg_ts"] = time.time()

                elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                  aiohttp.WSMsgType.ERROR):
                    log.warning("[Chainlink RTDS] Lost")
                    break

        except asyncio.CancelledError:
            log.info("[Chainlink RTDS] остановлен")
            raise
        except Exception as e:
            _status["last_error"] = str(e)
            log.warning(f"[Chainlink RTDS] err: {e}")
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

        await asyncio.sleep(5)


def get_window_candle(symbol: str, window_start: float, window_end: float,
                      timeframe: str = "5m") -> dict | None:
    """Свеча окна Polymarket из TWAP-тиков Chainlink.

    open  — первый тик со временем >= начала окна (это и есть «price to beat»);
    close — последний тик со временем <= конца окна (или последний вообще,
            если окно ещё идёт).

    Возвращает None, если тиков по этому окну ещё нет — тогда вызывающий код
    сам решит, брать ли запасной источник.
    """
    sym = to_rtds_symbol(symbol)
    window_s = twap_window_for(timeframe)
    arr = _ticks.get((sym, window_s))
    if not arr:
        return None

    now = time.time()
    end_cap = min(now, window_end)

    open_price = None
    open_ts = None
    close_price = None
    close_ts = None
    high = low = None

    for ts, price in arr:
        if ts < window_start:
            continue
        if ts > end_cap:
            break
        if open_price is None:
            open_price, open_ts = price, ts
        close_price, close_ts = price, ts
        high = price if high is None else max(high, price)
        low = price if low is None else min(low, price)

    if open_price is None or close_price is None:
        return None

    # Свеча считается закрытой, когда окно прошло и у нас есть тик у самого
    # его конца (RTDS шлёт примерно раз в секунду).
    closed = now >= window_end and (window_end - close_ts) <= 3.0

    return {
        "t": window_start,
        "open": open_price,
        "close": close_price,
        "high": high if high is not None else open_price,
        "low": low if low is not None else open_price,
        "closed": closed,
        "src": f"chainlink_twap{window_s}",
        "ticks": sum(1 for ts, _ in arr if window_start <= ts <= end_cap),
        "open_ts": open_ts,
        "close_ts": close_ts,
    }


def get_recent_candles(symbol: str, timeframe: str, count: int,
                       before_ts: float | None = None) -> list:
    """Последние `count` ЗАКРЫТЫХ свечей окон (по возрастанию времени)."""
    from_ts = before_ts or time.time()
    dur = 300
    if timeframe == "15m":
        dur = 900
    elif timeframe == "1h":
        dur = 3600

    cur_start = (int(from_ts) // dur) * dur
    out = []
    for i in range(count, 0, -1):
        ws = cur_start - i * dur
        cndl = get_window_candle(symbol, ws, ws + dur, timeframe)
        if cndl:
            out.append(cndl)
    return out


def have_data(symbol: str, timeframe: str = "5m") -> bool:
    sym = to_rtds_symbol(symbol)
    arr = _ticks.get((sym, twap_window_for(timeframe)))
    return bool(arr) and (time.time() - arr[-1][0]) < 120


def last_price(symbol: str, timeframe: str = "5m"):
    sym = to_rtds_symbol(symbol)
    arr = _ticks.get((sym, twap_window_for(timeframe)))
    return arr[-1][1] if arr else None


def status() -> dict:
    last = _status.get("last_msg_ts") or 0
    return {
        "connected": bool(_status.get("connected")),
        "updates": int(_status.get("updates", 0)),
        "symbols": len({k[0] for k in _ticks}),
        "age_sec": (time.time() - last) if last else None,
        "last_error": _status.get("last_error", ""),
    }
