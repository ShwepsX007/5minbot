import asyncio
import time
import json
from typing import Optional
import aiohttp

# ============== КЭШИ ==============
_gate_contract_specs = {}
_gate_contract_specs_ts = {}

_okx_specs = {}
_okx_specs_ts = {}

_CONTRACT_CACHE_TTL = 3600

_last_seen = {
    'gate': {},
    'okx': {},
    'bybit': {},
    'binance': {},
}

# Кэш топ-символов
_top_symbols = []
_top_symbols_ts = 0
_TOP_SYMBOLS_TTL = 3600

# Bybit WS
_bybit_ws_events = []
_bybit_ws_lock = asyncio.Lock()
_bybit_desired_symbols = set()
_bybit_subscribed_topics = set()
_bybit_topic_blacklist = set()
_bybit_supported_symbols = set()
_bybit_supported_ts = 0
_bybit_last_plan_signature = None
_BYBIT_SUPPORTED_TTL = 21600

GATE_BASE = "https://api.gateio.ws/api/v4/futures/usdt"
OKX_BASE = "https://www.okx.com/api/v5/public"
BYBIT_REST_BASE = "https://api.bybit.com/v5/market"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
BINANCE_FAPI = "https://fapi.binance.com"  # USDT-M Futures forceOrders endpoint


# ============== ХЕЛПЕРЫ ==============

def normalize_symbol(symbol: str, exchange: str) -> str:
    s = symbol.upper()
    s = s.replace('-SWAP', '')
    s = s.replace('-', '_')
    if exchange == 'bybit':
        if s.endswith('USDT') and '_' not in s:
            s = s[:-4] + '_USDT'
    return s


async def fetch_json(
        session: aiohttp.ClientSession,
        url: str,
        params: dict = None) -> Optional[list | dict]:
    try:
        async with session.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status == 200:
                return await resp.json()
            return None
    except Exception:
        return None


# ============== ТОП СИМВОЛЫ ==============

async def get_top_symbols(
        session: aiohttp.ClientSession,
        top_n: int = 30) -> list:
    global _top_symbols, _top_symbols_ts

    now = time.time()
    if (_top_symbols and
            now - _top_symbols_ts < _TOP_SYMBOLS_TTL):
        return _top_symbols

    print(f"[liq_api] Обновляем топ-{top_n}...")

    url = f"{GATE_BASE}/tickers"
    data = await fetch_json(session, url)

    if not data:
        if _top_symbols:
            return _top_symbols
        return _get_default_symbols()

    try:
        usdt_pairs = [
            t for t in data
            if t.get('contract', '').endswith('_USDT')
            and not t.get('contract', '').startswith(
                ('XAU_', 'XAG_'))
        ]

        sorted_pairs = sorted(
            usdt_pairs,
            key=lambda x: float(
                x.get('volume_24h_quote', 0) or 0),
            reverse=True)

        top = [
            t['contract']
            for t in sorted_pairs[:top_n]
            if t.get('contract')
        ]

        if top:
            _top_symbols = top
            _top_symbols_ts = now
            print(
                f"[liq_api] Топ-{top_n}: "
                f"{', '.join(top[:5])}... "
                f"и ещё {max(len(top) - 5, 0)}")
            return _top_symbols

    except Exception as e:
        print(f"[liq_api] top err: {e}")

    if _top_symbols:
        return _top_symbols
    return _get_default_symbols()


def _get_default_symbols() -> list:
    return [
        'BTC_USDT', 'ETH_USDT', 'SOL_USDT',
        'XRP_USDT', 'DOGE_USDT', 'BNB_USDT',
        'ADA_USDT', 'AVAX_USDT', 'LTC_USDT',
        'LINK_USDT', 'DOT_USDT', 'NEAR_USDT',
        'BCH_USDT', 'INJ_USDT', 'TIA_USDT',
        'APT_USDT', 'ARB_USDT', 'OP_USDT',
        'SUI_USDT', 'WIF_USDT',
    ]


# ==================================================
#                    GATE.IO
# ==================================================

async def get_gate_contract_spec(
        session, contract):
    now = time.time()
    if (contract in _gate_contract_specs and
            now - _gate_contract_specs_ts.get(
                contract, 0) < _CONTRACT_CACHE_TTL):
        return _gate_contract_specs[contract]

    url = f"{GATE_BASE}/contracts/{contract}"
    data = await fetch_json(session, url)

    if data and 'quanto_multiplier' in data:
        _gate_contract_specs[contract] = data
        _gate_contract_specs_ts[contract] = now
        return data
    return None


async def get_gate_liquidations(
        session, contract, min_usd=10000):

    spec = await get_gate_contract_spec(
        session, contract)
    if not spec:
        return []

    quanto = float(
        spec.get('quanto_multiplier', 0.0001))

    url = f"{GATE_BASE}/liq_orders"
    orders = await fetch_json(
        session, url,
        params={'contract': contract, 'limit': 100})

    if not orders:
        return []

    last_ts = _last_seen['gate'].get(contract, 0)
    events = []
    max_ts = last_ts

    for o in orders:
        ts = o.get('time', 0)
        if ts <= last_ts:
            continue

        size = abs(o.get('size', 0))
        order_size = o.get('order_size', 0)
        fill_price = float(o.get('fill_price', 0))

        if size == 0 or fill_price == 0:
            continue

        usd = size * quanto * fill_price
        if usd < min_usd:
            continue

        events.append({
            'exchange': 'Gate.io',
            'symbol': normalize_symbol(
                contract, 'gate'),
            'time': ts,
            'direction': (
                "LONG" if order_size < 0
                else "SHORT"),
            'usd_value': usd,
            'price': fill_price,
        })

        if ts > max_ts:
            max_ts = ts

    if max_ts > last_ts:
        _last_seen['gate'][contract] = max_ts

    return events


# ==================================================
#                    OKX
# ==================================================

async def get_okx_spec(session, base):
    inst_id = f"{base}-USDT-SWAP"
    now = time.time()

    if (inst_id in _okx_specs and
            now - _okx_specs_ts.get(inst_id, 0)
            < _CONTRACT_CACHE_TTL):
        return _okx_specs[inst_id]

    url = f"{OKX_BASE}/instruments"
    data = await fetch_json(
        session, url,
        params={'instType': 'SWAP', 'instId': inst_id})

    if not data:
        return None

    items = data.get('data', [])
    if not items:
        return None

    spec = items[0]
    _okx_specs[inst_id] = spec
    _okx_specs_ts[inst_id] = now
    return spec


def calc_okx_usd_value(sz, px, spec):
    ct_val = float(spec.get('ctVal', 1))
    ct_val_ccy = str(
        spec.get('ctValCcy', '')).upper()

    if ct_val_ccy in ('USDT', 'USD', 'USDC'):
        return sz * ct_val
    return sz * ct_val * px


async def get_okx_liquidations(
        session, base, min_usd=10000):

    spec = await get_okx_spec(session, base)
    if not spec:
        return []

    inst_family = f"{base}-USDT"

    url = f"{OKX_BASE}/liquidation-orders"
    data = await fetch_json(
        session, url,
        params={
            'instType': 'SWAP',
            'instFamily': inst_family,
            'state': 'filled',
            'limit': '100',
        })

    if not data:
        return []

    liq_data = data.get('data', [])
    if not liq_data:
        return []

    last_ts = _last_seen['okx'].get(inst_family, 0)
    events = []
    max_ts = last_ts

    for batch in liq_data:
        details = batch.get('details', [])
        for d in details:
            ts = int(d.get('ts', 0)) / 1000
            if ts <= last_ts:
                continue

            sz = float(d.get('sz', 0))
            px = float(d.get('bkPx', 0))
            if sz == 0 or px == 0:
                continue

            usd = calc_okx_usd_value(sz, px, spec)
            if usd < min_usd:
                continue

            events.append({
                'exchange': 'OKX',
                'symbol': f"{base}_USDT",
                'time': ts,
                'direction': (
                    "LONG" if d.get('side') == "sell"
                    else "SHORT"),
                'usd_value': usd,
                'price': px,
            })

            if ts > max_ts:
                max_ts = ts

    if max_ts > last_ts:
        _last_seen['okx'][inst_family] = max_ts

    return events


# ==================================================
#           OPEN INTEREST (мульти-биржа)
# ==================================================

async def get_multi_oi(
        session: aiohttp.ClientSession,
        base_symbol: str) -> dict:
    """
    Получить OI со всех трёх бирж.
    Возвращает:
    {
        'Gate.io': 210000000,
        'Bybit': 180000000,
        'OKX': 95000000,
        'total': 485000000,
    }
    """
    base = base_symbol.replace('_USDT', '')

    results = await asyncio.gather(
        _get_gate_oi_usd(session, base_symbol),
        _get_bybit_oi_usd(session, f"{base}USDT"),
        _get_okx_oi_usd(session, base),
        return_exceptions=True,
    )

    oi = {}
    total = 0

    labels = ['Gate.io', 'Bybit', 'OKX']
    for i, label in enumerate(labels):
        val = results[i]
        if isinstance(val, (int, float)) and val > 0:
            oi[label] = val
            total += val

    oi['total'] = total
    return oi


async def _get_gate_oi_usd(session, contract):
    """Gate OI — уже в USD из contract_stats."""
    url = f"{GATE_BASE}/contract_stats"
    data = await fetch_json(
        session, url,
        params={
            'contract': contract,
            'interval': '5m',
            'limit': 1
        })

    if not data or not isinstance(data, list):
        return 0

    if len(data) == 0:
        return 0

    return float(
        data[-1].get('open_interest_usd', 0) or 0)


async def _get_bybit_oi_usd(session, symbol):
    """Bybit OI — в базовом активе, надо × markPrice."""
    # 1. OI
    oi_url = f"{BYBIT_REST_BASE}/open-interest"
    oi_data = await fetch_json(
        session, oi_url,
        params={
            'category': 'linear',
            'symbol': symbol,
            'intervalTime': '5min',
            'limit': 1
        })

    if not oi_data:
        return 0

    oi_list = oi_data.get(
        'result', {}).get('list', [])
    if not oi_list:
        return 0

    open_interest = float(
        oi_list[0].get('openInterest', 0))
    if open_interest <= 0:
        return 0

    # 2. markPrice
    ticker_url = f"{BYBIT_REST_BASE}/tickers"
    ticker_data = await fetch_json(
        session, ticker_url,
        params={
            'category': 'linear',
            'symbol': symbol
        })

    if not ticker_data:
        return 0

    ticker_list = ticker_data.get(
        'result', {}).get('list', [])
    if not ticker_list:
        return 0

    mark_price = float(
        ticker_list[0].get('markPrice', 0))
    if mark_price <= 0:
        return 0

    return open_interest * mark_price


async def _get_okx_oi_usd(session, base):
    """OKX OI — через open-interest endpoint."""
    inst_id = f"{base}-USDT-SWAP"

    url = f"{OKX_BASE}/open-interest"
    data = await fetch_json(
        session, url,
        params={
            'instType': 'SWAP',
            'instId': inst_id,
        })

    if not data:
        return 0

    items = data.get('data', [])
    if not items:
        return 0

    # OKX OI в контрактах, нужен ctVal и markPrice
    oi_contracts = float(items[0].get('oi', 0))
    if oi_contracts <= 0:
        return 0

    # Берём спецификацию для ctVal
    spec = await get_okx_spec(session, base)
    if not spec:
        return 0

    ct_val = float(spec.get('ctVal', 1))
    ct_val_ccy = str(
        spec.get('ctValCcy', '')).upper()

    # Берём цену для конвертации
    # Используем markPrice из openInterest response
    # или из отдельного запроса
    mark_url = f"{OKX_BASE}/../market/ticker"
    # OKX не даёт markPrice в OI endpoint,
    # используем приблизительно
    # через instruments или ticker

    # Попробуем взять из mark-price endpoint
    mp_url = (
        "https://www.okx.com/api/v5/public/"
        "mark-price")
    mp_data = await fetch_json(
        session, mp_url,
        params={
            'instType': 'SWAP',
            'instId': inst_id
        })

    mark_price = 0
    if mp_data:
        mp_items = mp_data.get('data', [])
        if mp_items:
            mark_price = float(
                mp_items[0].get('markPx', 0))

    if mark_price <= 0:
        return 0

    if ct_val_ccy in ('USDT', 'USD', 'USDC'):
        return oi_contracts * ct_val
    else:
        return oi_contracts * ct_val * mark_price


# ==================================================
#                 BYBIT SYMBOLS
# ==================================================

async def _load_bybit_supported_symbols(session):
    global _bybit_supported_symbols
    global _bybit_supported_ts

    now = time.time()
    if (_bybit_supported_symbols and
            now - _bybit_supported_ts
            < _BYBIT_SUPPORTED_TTL):
        return _bybit_supported_symbols

    print("[Bybit WS] Загружаю supported symbols...")

    url = f"{BYBIT_REST_BASE}/instruments-info"
    collected = set()
    cursor = None

    while True:
        params = {'category': 'linear', 'limit': 1000}
        if cursor:
            params['cursor'] = cursor

        data = await fetch_json(
            session, url, params=params)
        if not data:
            break

        result = data.get('result', {})
        items = result.get('list', [])

        for item in items:
            sym = str(
                item.get('symbol', '')).upper()
            quote = str(
                item.get('quoteCoin', '')).upper()
            status = str(
                item.get('status', '')).lower()

            if (sym and quote == 'USDT' and
                    status in (
                        'trading', 'settling',
                        'deliverying')):
                collected.add(sym)

        cursor = result.get('nextPageCursor')
        if not cursor:
            break

    if collected:
        _bybit_supported_symbols = collected
        _bybit_supported_ts = now
        print(
            f"[Bybit WS] Supported: "
            f"{len(collected)}")

    return _bybit_supported_symbols


def set_bybit_symbols(symbols: list):
    global _bybit_desired_symbols
    desired = set()
    for sym in symbols:
        if sym:
            desired.add(
                sym.replace('_', '').upper())
    _bybit_desired_symbols = desired


def _extract_failed_topic(ret_msg):
    if not ret_msg:
        return None
    marker = "topic:"
    if marker not in ret_msg:
        return None
    return ret_msg.split(marker, 1)[1].strip()


async def _bybit_subscribe_pending(ws, session):
    global _bybit_subscribed_topics
    global _bybit_last_plan_signature

    supported = await _load_bybit_supported_symbols(
        session)

    desired_topics = {
        f"allLiquidation.{sym}"
        for sym in _bybit_desired_symbols
        if sym in supported
    }

    desired_topics = {
        t for t in desired_topics
        if t not in _bybit_topic_blacklist
    }

    pending = sorted([
        t for t in desired_topics
        if t not in _bybit_subscribed_topics
    ])

    skipped = sorted([
        sym for sym in _bybit_desired_symbols
        if f"allLiquidation.{sym}"
        not in desired_topics
    ])

    sig = (
        len(_bybit_desired_symbols),
        len(desired_topics),
        tuple(skipped[:10]),
    )

    if sig != _bybit_last_plan_signature:
        _bybit_last_plan_signature = sig
        if skipped:
            print(
                f"[Bybit WS] valid: "
                f"{len(desired_topics)}/"
                f"{len(_bybit_desired_symbols)}; "
                f"skip: {', '.join(skipped[:6])}"
                f"{'...' if len(skipped) > 6 else ''}"
            )
        else:
            print(
                f"[Bybit WS] valid: "
                f"{len(desired_topics)}/"
                f"{len(_bybit_desired_symbols)}")

    for topic in pending:
        try:
            await ws.send_json({
                "op": "subscribe",
                "args": [topic]
            })
            _bybit_subscribed_topics.add(topic)
            print(f"[Bybit WS] sub: {topic}")
            await asyncio.sleep(0.1)
        except Exception as e:
            print(f"[Bybit WS] sub err: {e}")


# ==================================================
#                 BYBIT WS
# ==================================================

async def bybit_ws_listener():
    global _bybit_ws_events
    global _bybit_subscribed_topics
    global _bybit_last_plan_signature

    while True:
        session = None
        ws = None

        try:
            print("[Bybit WS] Connecting...")

            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(
                    total=None))

            await _load_bybit_supported_symbols(
                session)

            ws = await session.ws_connect(
                BYBIT_WS, heartbeat=20, autoping=True)

            print("[Bybit WS] Connected!")
            _bybit_subscribed_topics = set()
            _bybit_last_plan_signature = None

            await _bybit_subscribe_pending(
                ws, session)

            last_sync = 0

            while True:
                now = time.time()
                if now - last_sync > 15:
                    await _bybit_subscribe_pending(
                        ws, session)
                    last_sync = now

                try:
                    msg = await asyncio.wait_for(
                        ws.receive(), timeout=5)
                except asyncio.TimeoutError:
                    continue

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except Exception:
                        continue

                    if data.get('op') == 'subscribe':
                        if data.get('success'):
                            print(
                                "[Bybit WS] Sub OK")
                        else:
                            ret = data.get(
                                'ret_msg', '')
                            print(
                                "[Bybit WS] Sub err:"
                                f" {ret}")
                            ft = _extract_failed_topic(
                                ret)
                            if ft:
                                _bybit_topic_blacklist\
                                    .add(ft)
                                _bybit_subscribed_topics\
                                    .discard(ft)
                        continue

                    topic = data.get('topic', '')
                    if not topic.startswith(
                            'allLiquidation.'):
                        continue

                    raw = data.get('data', [])
                    if isinstance(raw, dict):
                        raw = [raw]

                    parsed = []
                    for item in raw:
                        ev = _parse_bybit_event(item)
                        if ev:
                            parsed.append(ev)

                    if not parsed:
                        continue

                    async with _bybit_ws_lock:
                        _bybit_ws_events.extend(
                            parsed)
                        cutoff = time.time() - 600
                        _bybit_ws_events = [
                            e for e in
                            _bybit_ws_events
                            if e['time'] >= cutoff
                        ]

                    for ev in parsed:
                        if ev['usd_value'] >= 50000:
                            print(
                                f"[Bybit WS] "
                                f"{ev['symbol']} "
                                f"{ev['direction']} "
                                f"${ev['usd_value']:,.0f}")

                elif msg.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR):
                    print("[Bybit WS] Lost")
                    break

        except asyncio.CancelledError:
            print("[Bybit WS] остановлен")
            raise
        except Exception as e:
            print(f"[Bybit WS] err: {e}")
        finally:
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

        print("[Bybit WS] Reconnect 5s...")
        await asyncio.sleep(5)


def _parse_bybit_event(item):
    try:
        sym = item.get('s') or item.get('symbol')
        side = item.get('S') or item.get('side')
        qty = float(
            item.get('v') or item.get('size') or 0)
        price = float(
            item.get('p') or item.get('price') or 0)
        ts = item.get('T') or item.get('time')

        if not sym or not side:
            return None
        if qty == 0 or price == 0:
            return None

        if ts:
            ts = float(ts)
            if ts > 10_000_000_000:
                ts = ts / 1000
        else:
            ts = time.time()

        return {
            'exchange': 'Bybit',
            'symbol': normalize_symbol(sym, 'bybit'),
            'time': ts,
            'direction': (
                "LONG"
                if str(side).lower() == "sell"
                else "SHORT"),
            'usd_value': qty * price,
            'price': price,
        }
    except Exception:
        return None


async def get_bybit_liquidations(
        base_symbol, min_usd=10000):
    target = base_symbol
    last_ts = _last_seen['bybit'].get(target, 0)

    async with _bybit_ws_lock:
        events = [
            e for e in _bybit_ws_events
            if e['symbol'] == target
            and e['usd_value'] >= min_usd
            and e['time'] > last_ts
        ]

    if events:
        _last_seen['bybit'][target] = max(
            e['time'] for e in events)

    return events


# ==================================================
#                    BINANCE
# ==================================================

async def get_binance_liquidations(
        session, base_symbol, min_usd=10000):
    """Ликвидации с Binance USDT-M Futures.

    Источник: GET https://fapi.binance.com/fapi/v1/forceOrders
    Возвращает только USDT-M (linear) контракты. Без API ключей,
    но endpoint публичный. Binance — крупнейшая биржа по объёмам,
    у них самые большие ликвидационные каскады, поэтому подключение
    этой биржи критично для детекции крупных каскадов.

    Поддерживает lookback только до 7 дней, после чего Binance
    очищает историю. Дельта-фильтрация через _last_seen['binance']
    работает корректно даже после перезапуска бота: при первом
    запуске last_ts=0 и бот получит все ликвидации за последние
    100 записей — обычно это последние минуты/часы.

    Формат forceOrders (документация Binance):
    {
      "symbol": "BTCUSDT",
      "orderId": 12345,
      "price": "30000.00",
      "qty": "0.500",
      "side": "BUY" | "SELL",   # сторона ордера ликвидации
      "time": 1700000000000,    # ms epoch
      "avgPrice": "30000.00",
      "executedQty": "0.500",
      ...
    }
    Сторона "BUY" = ликвидация SHORT позиции (forced buyback)
    Сторона "SELL" = ликвидация LONG позиции (forced sell)
    Поэтому в нашей логике:
      side SELL → liquidated_long → direction "LONG"
      side BUY  → liquidated_short → direction "SHORT"
    """
    symbol = base_symbol.replace('_USDT', '') + 'USDT'
    last_ts = _last_seen['binance'].get(base_symbol, 0)

    url = f"{BINANCE_FAPI}/fapi/v1/forceOrders"
    data = await fetch_json(
        session, url,
        params={'symbol': symbol, 'limit': 100}
    )

    if not data or not isinstance(data, list):
        return []

    events = []
    max_ts = last_ts

    for o in data:
        # Binance отдаёт time в миллисекундах
        ts_ms = o.get('time', 0)
        if not ts_ms:
            continue
        ts = float(ts_ms) / 1000.0
        # Дельта-фильтрация (time в секундах, last_ts в секундах)
        if ts <= last_ts:
            continue

        try:
            price = float(o.get('avgPrice') or o.get('price') or 0)
            qty = float(o.get('executedQty') or o.get('qty') or 0)
        except (TypeError, ValueError):
            continue
        if price <= 0 or qty <= 0:
            continue

        usd = price * qty
        if usd < min_usd:
            continue

        side = str(o.get('side', '')).upper()
        # SELL = закрытие LONG (forced sell) → direction "LONG"
        # BUY  = закрытие SHORT (forced buy) → direction "SHORT"
        direction = "LONG" if side == "SELL" else "SHORT"

        events.append({
            'exchange': 'Binance',
            'symbol': base_symbol,
            'time': ts,
            'direction': direction,
            'usd_value': usd,
            'price': price,
        })

        if ts > max_ts:
            max_ts = ts

    if max_ts > last_ts:
        _last_seen['binance'][base_symbol] = max_ts

    return events


# ==================================================
#                 АГРЕГАТОР
# ==================================================

async def get_all_liquidations(
        session, base_symbol, min_usd=10000):
    base = base_symbol.replace('_USDT', '')

    results = await asyncio.gather(
        get_gate_liquidations(
            session, base_symbol, min_usd),
        get_okx_liquidations(
            session, base, min_usd),
        get_bybit_liquidations(
            base_symbol, min_usd),
        get_binance_liquidations(
            session, base_symbol, min_usd),
        return_exceptions=True,
    )

    all_events = []
    sources = ("Gate.io", "OKX", "Bybit", "Binance")
    for source, result in zip(sources, results):
        if isinstance(result, list):
            all_events.extend(result)
        elif isinstance(result, Exception):
            # Do not silently discard a failed source: strategy diagnostics
            # and server logs need to show why no signal was produced.
            print(f"[liq_api] {source} liquidation request failed: {result}")

    all_events.sort(
        key=lambda x: x.get('time', 0))
    return all_events


def aggregate_liquidations(events, window_sec=60):
    cutoff = time.time() - window_sec

    long_usd = 0.0
    short_usd = 0.0
    long_count = 0
    short_count = 0
    by_exchange = {}

    for e in events:
        if e['time'] < cutoff:
            continue

        ex = e['exchange']
        if ex not in by_exchange:
            by_exchange[ex] = {
                'long_usd': 0.0,
                'short_usd': 0.0,
                'long_count': 0,
                'short_count': 0,
            }

        if e['direction'] == 'LONG':
            long_usd += e['usd_value']
            long_count += 1
            by_exchange[ex]['long_usd'] += (
                e['usd_value'])
            by_exchange[ex]['long_count'] += 1
        else:
            short_usd += e['usd_value']
            short_count += 1
            by_exchange[ex]['short_usd'] += (
                e['usd_value'])
            by_exchange[ex]['short_count'] += 1

    total = long_usd + short_usd

    if long_usd > short_usd * 1.5:
        dominant = 'LONG'
    elif short_usd > long_usd * 1.5:
        dominant = 'SHORT'
    else:
        dominant = 'NEUTRAL'

    return {
        'long_liq_usd': long_usd,
        'short_liq_usd': short_usd,
        'total_usd': total,
        'long_count': long_count,
        'short_count': short_count,
        'dominant': dominant,
        'by_exchange': by_exchange,
    }


async def get_contract_stats(
        session, contract,
        interval='5m', limit=3):
    url = f"{GATE_BASE}/contract_stats"
    return await fetch_json(
        session, url,
        params={
            'contract': contract,
            'interval': interval,
            'limit': limit
        })

# ==================================================
#         МУЛЬТИ-БИРЖА: LSR + FUNDING
# ==================================================

async def get_multi_lsr(
        session: aiohttp.ClientSession,
        base_symbol: str) -> dict:
    """
    Получить LSR со всех бирж.
    Возвращает:
    {
        'Gate.io': 1.24,
        'Bybit': 1.18,
        'OKX': 1.31,
        'average': 1.24,
    }
    """
    base = base_symbol.replace('_USDT', '')

    results = await asyncio.gather(
        _get_gate_lsr(session, base_symbol),
        _get_bybit_lsr(session, f"{base}USDT"),
        _get_okx_lsr(session, base),
        return_exceptions=True,
    )

    lsr = {}
    values = []
    labels = ['Gate.io', 'Bybit', 'OKX']

    for i, label in enumerate(labels):
        val = results[i]
        if isinstance(val, (int, float)) and val > 0:
            lsr[label] = round(val, 2)
            values.append(val)

    if values:
        lsr['average'] = round(
            sum(values) / len(values), 2
        )
    else:
        lsr['average'] = 0

    return lsr


async def _get_gate_lsr(session, contract):
    url = f"{GATE_BASE}/contract_stats"
    data = await fetch_json(
        session, url,
        params={
            'contract': contract,
            'interval': '5m',
            'limit': 1
        }
    )

    if not data or not isinstance(data, list):
        return 0
    if len(data) == 0:
        return 0

    return float(
        data[-1].get('lsr_taker', 0) or 0
    )


async def _get_bybit_lsr(session, symbol):
    url = f"{BYBIT_REST_BASE}/account-ratio"
    data = await fetch_json(
        session, url,
        params={
            'category': 'linear',
            'symbol': symbol,
            'period': '5min',
            'limit': 1
        }
    )

    if not data:
        return 0

    items = data.get('result', {}).get('list', [])
    if not items:
        return 0

    buy_ratio = float(
        items[0].get('buyRatio', 0)
    )
    sell_ratio = float(
        items[0].get('sellRatio', 0)
    )

    if sell_ratio > 0:
        return buy_ratio / sell_ratio

    return 0


async def _get_okx_lsr(session, base):
    inst_id = f"{base}-USDT-SWAP"

    url = (
        "https://www.okx.com/api/v5/rubik/stat/"
        "contracts/long-short-account-ratio/"
        "contract-top-trader"
    )

    data = await fetch_json(
        session, url,
        params={
            'instId': inst_id,
            'period': '5m',
        }
    )

    if not data:
        return 0

    items = data.get('data', [])
    if not items:
        return 0

    long_ratio = float(
        items[0].get('longShortRatio', 0)
    )

    return long_ratio


async def get_multi_funding(
        session: aiohttp.ClientSession,
        base_symbol: str) -> dict:
    """
    Получить Funding Rate со всех бирж.
    Возвращает:
    {
        'Gate.io': 0.0012,
        'Bybit': 0.0015,
        'OKX': 0.0010,
        'average': 0.0012,
    }
    """
    base = base_symbol.replace('_USDT', '')

    results = await asyncio.gather(
        _get_gate_funding(session, base_symbol),
        _get_bybit_funding(session, f"{base}USDT"),
        _get_okx_funding(session, base),
        return_exceptions=True,
    )

    funding = {}
    values = []
    labels = ['Gate.io', 'Bybit', 'OKX']

    for i, label in enumerate(labels):
        val = results[i]
        if isinstance(val, (int, float)):
            funding[label] = val
            values.append(val)

    if values:
        funding['average'] = sum(values) / len(values)
    else:
        funding['average'] = 0

    return funding


async def _get_gate_funding(session, contract):
    from bot.loader import gate_futures

    try:
        base, quote = contract.split('_')
        ccxt_symbol = f"{base}/{quote}:{quote}"
        data = await gate_futures.fetch_funding_rate(
            ccxt_symbol
        )
        return float(
            data.get('fundingRate', 0) or 0
        )
    except Exception:
        return 0


async def _get_bybit_funding(session, symbol):
    url = f"{BYBIT_REST_BASE}/tickers"
    data = await fetch_json(
        session, url,
        params={
            'category': 'linear',
            'symbol': symbol
        }
    )

    if not data:
        return 0

    items = data.get('result', {}).get('list', [])
    if not items:
        return 0

    return float(
        items[0].get('fundingRate', 0) or 0
    )


async def _get_okx_funding(session, base):
    inst_id = f"{base}-USDT-SWAP"

    url = (
        "https://www.okx.com/api/v5/public/"
        "funding-rate"
    )

    data = await fetch_json(
        session, url,
        params={'instId': inst_id}
    )

    if not data:
        return 0

    items = data.get('data', [])
    if not items:
        return 0

    return float(
        items[0].get('fundingRate', 0) or 0
    )


async def get_multi_oi_change(
        session: aiohttp.ClientSession,
        base_symbol: str) -> dict:
    """
    Получить изменение OI со всех трёх бирж.
    Возвращает:
    {
        'Gate.io': -0.45,
        'Bybit': -0.32,
        'OKX': -0.28,
        'average': -0.35,
    }
    """
    base = base_symbol.replace('_USDT', '')

    results = await asyncio.gather(
        _get_gate_oi_change(session, base_symbol),
        _get_bybit_oi_change(
            session, f"{base}USDT"),
        _get_okx_oi_change(session, base),
        return_exceptions=True,
    )

    changes = {}
    values = []
    labels = ['Gate.io', 'Bybit', 'OKX']

    for i, label in enumerate(labels):
        val = results[i]
        if (isinstance(val, (int, float))
                and val is not None):
            changes[label] = round(val, 3)
            values.append(val)

    if values:
        changes['average'] = round(
            sum(values) / len(values), 3
        )
    else:
        changes['average'] = 0

    return changes


async def _get_gate_oi_change(session, contract):
    """
    Gate OI change через contract_stats.
    Берём 2 последних замера и считаем % изменения.
    """
    url = f"{GATE_BASE}/contract_stats"
    data = await fetch_json(
        session, url,
        params={
            'contract': contract,
            'interval': '5m',
            'limit': 3
        }
    )

    if not data or len(data) < 2:
        return None

    prev_oi = float(
        data[-2].get('open_interest_usd', 0) or 0
    )
    curr_oi = float(
        data[-1].get('open_interest_usd', 0) or 0
    )

    if prev_oi <= 0:
        return None

    return ((curr_oi - prev_oi) / prev_oi) * 100


async def _get_bybit_oi_change(session, symbol):
    """
    Bybit OI change.
    Берём 2 последних замера через open-interest endpoint.
    """
    url = f"{BYBIT_REST_BASE}/open-interest"
    data = await fetch_json(
        session, url,
        params={
            'category': 'linear',
            'symbol': symbol,
            'intervalTime': '5min',
            'limit': 2
        }
    )

    if not data:
        return None

    items = data.get(
        'result', {}).get('list', [])

    if len(items) < 2:
        return None

    # Bybit returns newest first
    curr = float(
        items[0].get('openInterest', 0))
    prev = float(
        items[1].get('openInterest', 0))

    if prev <= 0:
        return None

    return ((curr - prev) / prev) * 100


async def _get_okx_oi_change(session, base):
    """
    OKX OI change через open-interest-history.
    Берём 2 последних замера и считаем % изменения.
    """
    inst_id = f"{base}-USDT-SWAP"

    url = (
        "https://www.okx.com/api/v5/rubik/stat/"
        "contracts/open-interest-history"
    )

    data = await fetch_json(
        session, url,
        params={
            'instId': inst_id,
            'period': '5m',
            'limit': '2',
        }
    )

    if not data:
        return None

    items = data.get('data', [])
    if len(items) < 2:
        return None

    # OKX также возвращает новые первыми
    try:
        curr_oi = float(items[0][1])
        prev_oi = float(items[1][1])
    except (IndexError, TypeError, ValueError):
        return None

    if prev_oi <= 0:
        return None

    return ((curr_oi - prev_oi) / prev_oi) * 100
