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

# Binance WS (публичный стрим ликвидаций !forceOrder@arr).
# REST /fapi/v1/forceOrders требует HMAC-подписи (USER_DATA) и без ключей
# отдаёт пустой ответ/ошибку, поэтому источник — только WS.
_binance_ws_events = []
_binance_ws_lock = asyncio.Lock()
_binance_desired_symbols = set()   # {'BTC_USDT', ...}; пусто = принимать всё

# Gate.io WS (публичный канал futures.public_liquidates).
# REST /futures/usdt/liq_orders в актуальной версии API также закрыт,
# поэтому источник — только WS.
_gate_ws_events = []
_gate_ws_lock = asyncio.Lock()
_gate_desired_symbols = set()      # {'BTC_USDT', ...}
_gate_subscribed_symbols = set()
_gate_ws_ref = {"ws": None}        # текущее соединение для доп. подписок

# Сколько секунд держим события в WS-буферах (буфер стратегии — 600с)
_WS_BUFFER_TTL = 600

# Диагностика WS-соединений (видна в статусе стратегии / логах)
_ws_status = {
    'bybit': {'connected': False, 'last_event_ts': 0.0, 'events': 0},
    'binance': {'connected': False, 'last_event_ts': 0.0, 'events': 0},
    'gate': {'connected': False, 'last_event_ts': 0.0, 'events': 0},
}

GATE_BASE = "https://api.gateio.ws/api/v4/futures/usdt"
OKX_BASE = "https://www.okx.com/api/v5/public"
BYBIT_REST_BASE = "https://api.bybit.com/v5/market"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
BINANCE_FAPI = "https://fapi.binance.com"

# Binance перенёс публичные стримы на /market (legacy /ws отключается),
# поэтому пробуем новый адрес, а при неудаче — старый.
BINANCE_WS_URLS = (
    "wss://fstream.binance.com/market/ws/!forceOrder@arr",
    "wss://fstream.binance.com/ws/!forceOrder@arr",
)

GATE_WS = "wss://fx-ws.gateio.ws/v4/ws/usdt"
GATE_LIQ_CHANNEL = "futures.public_liquidates"


# ============== ХЕЛПЕРЫ ==============

def normalize_symbol(symbol: str, exchange: str) -> str:
    s = symbol.upper()
    s = s.replace('-SWAP', '')
    s = s.replace('-', '_')
    if exchange in ('bybit', 'binance', 'gate'):
        # BTCUSDT -> BTC_USDT (Gate уже отдаёт с подчёркиванием)
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
        session=None, contract=None, min_usd=10000):
    """Ликвидации Gate.io из буфера публичного WS.

    Источник — канал futures.public_liquidates на
    wss://fx-ws.gateio.ws/v4/ws/usdt (без авторизации).
    REST /futures/usdt/liq_orders закрыт (нужны ключи и подпись),
    поэтому здесь только чтение буфера, который наполняет
    gate_ws_listener().

    Аргумент session сохранён для обратной совместимости с
    get_all_liquidations() и не используется.
    """
    if not contract:
        return []

    target = normalize_symbol(contract, 'gate')
    last_ts = _last_seen['gate'].get(target, 0)

    async with _gate_ws_lock:
        events = [
            e for e in _gate_ws_events
            if e['symbol'] == target
            and e['usd_value'] >= min_usd
            and e['time'] > last_ts
        ]

    if events:
        _last_seen['gate'][target] = max(
            e['time'] for e in events)

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
        _get_binance_oi_usd(session, f"{base}USDT"),
        return_exceptions=True,
    )

    oi = {}
    total = 0

    labels = ['Gate.io', 'Bybit', 'OKX', 'Binance']
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


async def _get_binance_oi_usd(session, symbol):
    """Binance OI в долларах.

    /fapi/v1/openInterest отдаёт OI в базовой монете (публичный, без
    подписи), поэтому умножаем на markPrice из /fapi/v1/premiumIndex.
    Если не вышло — берём готовое значение в USD из статистики
    /futures/data/openInterestHist (обновляется раз в 5 минут).
    """
    oi_data = await fetch_json(
        session, f"{BINANCE_FAPI}/fapi/v1/openInterest",
        params={'symbol': symbol})

    open_interest = 0.0
    if isinstance(oi_data, dict):
        try:
            open_interest = float(oi_data.get('openInterest') or 0)
        except (TypeError, ValueError):
            open_interest = 0.0

    if open_interest > 0:
        mark_data = await fetch_json(
            session, f"{BINANCE_FAPI}/fapi/v1/premiumIndex",
            params={'symbol': symbol})
        if isinstance(mark_data, list) and mark_data:
            mark_data = mark_data[0]
        if isinstance(mark_data, dict):
            try:
                mark = float(mark_data.get('markPrice') or 0)
            except (TypeError, ValueError):
                mark = 0.0
            if mark > 0:
                return open_interest * mark

    # Фолбэк: сумма OI в долларах из статистики
    hist = await fetch_json(
        session, f"{BINANCE_FAPI}/futures/data/openInterestHist",
        params={'symbol': symbol, 'period': '5m', 'limit': 1})
    if isinstance(hist, list) and hist:
        try:
            return float(hist[-1].get('sumOpenInterestValue') or 0)
        except (TypeError, ValueError):
            return 0
    return 0


async def _get_binance_oi_change(session, symbol):
    """Изменение OI за 5 минут в %.

    /futures/data/openInterestHist — публичный эндпоинт статистики
    (подпись не нужна), отдаёт записи от старых к новым.
    """
    data = await fetch_json(
        session, f"{BINANCE_FAPI}/futures/data/openInterestHist",
        params={'symbol': symbol, 'period': '5m', 'limit': 2})

    if not isinstance(data, list) or len(data) < 2:
        return None

    try:
        prev = float(data[0].get('sumOpenInterest') or 0)
        curr = float(data[-1].get('sumOpenInterest') or 0)
    except (TypeError, ValueError):
        return None

    if prev <= 0 or curr <= 0:
        return None
    return ((curr - prev) / prev) * 100


async def _get_binance_lsr(session, symbol):
    """Long/Short Ratio по счетам (публичная статистика Binance)."""
    data = await fetch_json(
        session, f"{BINANCE_FAPI}/futures/data/globalLongShortAccountRatio",
        params={'symbol': symbol, 'period': '5m', 'limit': 1})
    if not isinstance(data, list) or not data:
        return None
    try:
        return float(data[-1].get('longShortRatio') or 0)
    except (TypeError, ValueError):
        return None


async def _get_binance_funding(session, symbol):
    """Текущая ставка финансирования."""
    data = await fetch_json(
        session, f"{BINANCE_FAPI}/fapi/v1/premiumIndex",
        params={'symbol': symbol})
    if isinstance(data, list) and data:
        data = data[0]
    if not isinstance(data, dict):
        return 0
    try:
        return float(data.get('lastFundingRate') or 0)
    except (TypeError, ValueError):
        return 0


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
            _ws_status['bybit']['connected'] = True
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

                    _ws_status['bybit']['events'] += len(parsed)
                    _ws_status['bybit']['last_event_ts'] = (
                        time.time())

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
            _ws_status['bybit']['connected'] = False
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
#                    BINANCE (WS)
# ==================================================

def set_binance_symbols(symbols: list):
    """Список монет, которые кладём в буфер Binance WS.

    Стрим !forceOrder@arr отдаёт ликвидации по ВСЕМ символам биржи,
    поэтому фильтруем на входе, чтобы не раздувать память.
    Пустой список = принимать всё.
    """
    global _binance_desired_symbols
    desired = set()
    for sym in (symbols or []):
        if sym:
            desired.add(normalize_symbol(str(sym), 'binance'))
    _binance_desired_symbols = desired


def _parse_binance_ws_event(data):
    """Парсит сообщение стрима !forceOrder@arr.

    Формат (документация Binance USDⓈ-M Futures):
    {
      "e": "forceOrder",
      "E": 1591154240950,        # event time, ms
      "o": {
        "s": "BTCUSDT",          # symbol
        "S": "SELL",             # сторона ордера ликвидации
        "q": "0.014",            # исходное количество
        "p": "9425.5",           # цена ордера
        "ap": "9496.5",          # средняя цена исполнения
        "X": "FILLED",           # статус
        "l": "0.014",            # последнее исполненное количество
        "z": "0.014",            # накопленное исполненное количество
        "T": 1591154240949       # trade time, ms
      }
    }
    SELL = принудительная продажа  → ликвидирован LONG  → direction "LONG"
    BUY  = принудительная покупка → ликвидирован SHORT → direction "SHORT"
    """
    try:
        if not isinstance(data, dict):
            return None
        # combined-stream обёртка {"stream": ..., "data": {...}}
        if 'data' in data and 'e' not in data:
            data = data.get('data') or {}
        if data.get('e') != 'forceOrder':
            return None

        o = data.get('o') or {}
        sym = str(o.get('s') or '').upper()
        side = str(o.get('S') or '').upper()
        if not sym or not side:
            return None

        qty = 0.0
        for key in ('z', 'l', 'q'):
            try:
                qty = float(o.get(key) or 0)
            except (TypeError, ValueError):
                qty = 0.0
            if qty > 0:
                break

        price = 0.0
        for key in ('ap', 'p'):
            try:
                price = float(o.get(key) or 0)
            except (TypeError, ValueError):
                price = 0.0
            if price > 0:
                break

        if qty <= 0 or price <= 0:
            return None

        ts_ms = o.get('T') or data.get('E') or 0
        try:
            ts = float(ts_ms) / 1000.0 if ts_ms else time.time()
        except (TypeError, ValueError):
            ts = time.time()

        return {
            'exchange': 'Binance',
            'symbol': normalize_symbol(sym, 'binance'),
            'time': ts,
            'direction': "LONG" if side == "SELL" else "SHORT",
            'usd_value': qty * price,
            'price': price,
        }
    except Exception:
        return None


async def binance_ws_listener():
    """Публичный WS-стрим ликвидаций Binance USDⓈ-M Futures.

    wss://fstream.binance.com/market/ws/!forceOrder@arr — без ключей и
    подписи (REST /fapi/v1/forceOrders требует HMAC и потому не годится).
    Стрим отдаёт ликвидации по всем символам, снапшотом не чаще
    1 сообщения в секунду на символ. Автопереподключение — как у Bybit.
    """
    global _binance_ws_events

    url_idx = 0

    while True:
        session = None
        ws = None
        url = BINANCE_WS_URLS[url_idx % len(BINANCE_WS_URLS)]

        try:
            print(f"[Binance WS] Connecting {url} ...")

            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None))
            ws = await session.ws_connect(
                url, heartbeat=20, autoping=True)

            _ws_status['binance']['connected'] = True
            print("[Binance WS] Connected!")

            while True:
                try:
                    msg = await asyncio.wait_for(
                        ws.receive(), timeout=30)
                except asyncio.TimeoutError:
                    continue

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except Exception:
                        continue

                    raw = data
                    if isinstance(raw, list):
                        items = raw
                    else:
                        items = [raw]

                    parsed = []
                    for item in items:
                        ev = _parse_binance_ws_event(item)
                        if not ev:
                            continue
                        if (_binance_desired_symbols and
                                ev['symbol'] not in
                                _binance_desired_symbols):
                            continue
                        parsed.append(ev)

                    if not parsed:
                        continue

                    async with _binance_ws_lock:
                        _binance_ws_events.extend(parsed)
                        cutoff = time.time() - _WS_BUFFER_TTL
                        _binance_ws_events = [
                            e for e in _binance_ws_events
                            if e['time'] >= cutoff
                        ]

                    _ws_status['binance']['events'] += len(parsed)
                    _ws_status['binance']['last_event_ts'] = (
                        time.time())

                    for ev in parsed:
                        if ev['usd_value'] >= 50000:
                            print(
                                f"[Binance WS] "
                                f"{ev['symbol']} "
                                f"{ev['direction']} "
                                f"${ev['usd_value']:,.0f}")

                elif msg.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR):
                    print("[Binance WS] Lost")
                    # Возможно, адрес устарел — пробуем следующий
                    url_idx += 1
                    break

        except asyncio.CancelledError:
            print("[Binance WS] остановлен")
            raise
        except Exception as e:
            print(f"[Binance WS] err: {e}")
            url_idx += 1
        finally:
            _ws_status['binance']['connected'] = False
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

        print("[Binance WS] Reconnect 5s...")
        await asyncio.sleep(5)


async def get_binance_liquidations(
        session=None, base_symbol=None, min_usd=10000):
    """Ликвидации Binance из буфера публичного WS.

    Аргумент session оставлен для обратной совместимости с
    get_all_liquidations() и не используется.
    """
    if not base_symbol:
        return []

    target = normalize_symbol(base_symbol, 'binance')
    last_ts = _last_seen['binance'].get(target, 0)

    async with _binance_ws_lock:
        events = [
            e for e in _binance_ws_events
            if e['symbol'] == target
            and e['usd_value'] >= min_usd
            and e['time'] > last_ts
        ]

    if events:
        _last_seen['binance'][target] = max(
            e['time'] for e in events)

    return events


# ==================================================
#                   GATE.IO (WS)
# ==================================================

def set_gate_symbols(symbols: list):
    """Контракты, на которые подписывается Gate.io WS."""
    global _gate_desired_symbols
    desired = set()
    for sym in (symbols or []):
        if sym:
            desired.add(normalize_symbol(str(sym), 'gate'))
    _gate_desired_symbols = desired


def set_symbols(symbols: list):
    """Единая точка: задать монеты сразу для всех WS-источников."""
    set_bybit_symbols(symbols)
    set_binance_symbols(symbols)
    set_gate_symbols(symbols)


def ensure_symbol(symbol: str):
    """Добавляет монету к подпискам WS, не сбрасывая остальные."""
    if not symbol:
        return
    norm = normalize_symbol(str(symbol), 'gate')
    if norm not in _gate_desired_symbols:
        _gate_desired_symbols.add(norm)
    if _binance_desired_symbols and norm not in _binance_desired_symbols:
        _binance_desired_symbols.add(norm)
    bybit_sym = norm.replace('_', '')
    if bybit_sym not in _bybit_desired_symbols:
        _bybit_desired_symbols.add(bybit_sym)


async def recent_liquidations(symbol: str, min_usd: float = 1000.0,
                              since: float = 0.0) -> list:
    """Последние ликвидации монеты напрямую из WS-буферов (Bybit/Binance/Gate).

    В отличие от get_all_liquidations() НИКАКИХ курсоров `_last_seen` не
    двигает: читать можно из второй стратегии, не «съедая» новые события
    сканеру «Каскада ликвидаций». Буферы живут 600с — ровно два окна по
    5 минут, этого хватает для сравнения «окно vs предыдущее окно».
    OKX здесь не участвует (у него REST с курсором) — сравнение окон и без
    него репрезентативно, обе стратегии видят одинаковую картину.

    Возвращает события time >= since и usd_value >= min_usd, отсортированные
    по времени, с дедупликацией (источник, время, объём).
    """
    targets = {normalize_symbol(str(symbol), ex)
               for ex in ("bybit", "binance", "gate")}
    targets.add(str(symbol).upper())
    out, seen = [], set()
    stores = ((_bybit_ws_events, _bybit_ws_lock),
              (_binance_ws_events, _binance_ws_lock),
              (_gate_ws_events, _gate_ws_lock))
    for (store, lock), name in zip(stores, ("Bybit", "Binance", "Gate.io")):
        try:
            async with lock:
                raw = [e for e in store
                       if str(e.get("symbol", "")) in targets
                       and float(e.get("time", 0) or 0) >= since
                       and float(e.get("usd_value", 0) or 0) >= min_usd]
        except Exception:
            raw = []
        for e in raw:
            key = (name, e.get("time"),
                   round(float(e.get("usd_value", 0) or 0), 2))
            if key in seen:
                continue
            seen.add(key)
            out.append(e)
    out.sort(key=lambda e: float(e.get("time", 0) or 0))
    return out


def ws_liquid_ready() -> bool:
    """Хотя бы один WS-источник ликвидаций подключён.

    Нужен, чтобы отличать «ликвидаций нет — рынок.flat» (буферы живые,
    событий нет) от «данных просто нет» (WS упал/монета не подписана).
    """
    try:
        return any(bool(v.get("connected")) for v in _ws_status.values())
    except Exception:
        return False


async def _gate_quanto(session, contract):
    """quanto_multiplier контракта (размер 1 контракта в базовой монете)."""
    spec = await get_gate_contract_spec(session, contract)
    if not spec:
        return None
    try:
        q = float(spec.get('quanto_multiplier') or 0)
    except (TypeError, ValueError):
        return None
    return q if q > 0 else None


async def _gate_subscribe_pending(ws, session):
    """Досылает подписки на новые контракты (idempotent)."""
    global _gate_subscribed_symbols

    pending = sorted(
        _gate_desired_symbols - _gate_subscribed_symbols)
    if not pending:
        return

    for contract in pending:
        # Прогреваем кэш спецификации: без quanto_multiplier
        # мы не сможем пересчитать размер в USD.
        try:
            await _gate_quanto(session, contract)
        except Exception:
            pass

    try:
        await ws.send_json({
            "time": int(time.time()),
            "channel": GATE_LIQ_CHANNEL,
            "event": "subscribe",
            "payload": pending,
        })
        _gate_subscribed_symbols |= set(pending)
        print(
            f"[Gate WS] sub: {', '.join(pending)}")
    except Exception as e:
        print(f"[Gate WS] sub err: {e}")


async def _parse_gate_ws_events(session, data):
    """Парсит уведомление канала futures.public_liquidates.

    {
      "channel": "futures.public_liquidates",
      "event": "update",
      "time": 1541505434,
      "time_ms": 1541505434123,
      "result": [
        {"price": "215.1", "size": "-124.5",
         "time": 1541486601, "contract": "BTC_USDT"}
      ]
    }
    size < 0 — принудительная продажа → ликвидирован LONG.
    size > 0 — принудительная покупка → ликвидирован SHORT.
    size указан в КОНТРАКТАХ, поэтому объём в USD считаем как
    |size| * quanto_multiplier * price.
    """
    out = []
    result = data.get('result')
    if isinstance(result, dict):
        result = [result]
    if not isinstance(result, list):
        return out

    for item in result:
        if not isinstance(item, dict):
            continue
        contract = str(
            item.get('contract') or '').upper().replace('-', '_')
        if not contract:
            continue

        try:
            size = float(item.get('size') or 0)
            price = float(item.get('price') or 0)
        except (TypeError, ValueError):
            continue
        if size == 0 or price <= 0:
            continue

        quanto = await _gate_quanto(session, contract)
        if not quanto:
            # Без множителя объём посчитать нельзя — пропускаем,
            # чтобы не завышать/занижать каскад.
            continue

        ts_raw = (
            item.get('time_ms') or item.get('time') or
            data.get('time_ms') or data.get('time') or 0)
        try:
            ts = float(ts_raw)
            if ts > 10_000_000_000:
                ts = ts / 1000.0
        except (TypeError, ValueError):
            ts = time.time()
        if not ts:
            ts = time.time()

        out.append({
            'exchange': 'Gate.io',
            'symbol': normalize_symbol(contract, 'gate'),
            'time': ts,
            'direction': "LONG" if size < 0 else "SHORT",
            'usd_value': abs(size) * quanto * price,
            'price': price,
        })

    return out


async def gate_ws_listener():
    """Публичный WS Gate.io: канал futures.public_liquidates.

    wss://fx-ws.gateio.ws/v4/ws/usdt — авторизация не нужна
    (в отличие от REST /futures/usdt/liq_orders и приватного
    канала futures.liquidates). Подписка идёт на выбранные в
    стратегии контракты; список обновляется каждые 15 секунд.
    """
    global _gate_ws_events
    global _gate_subscribed_symbols

    while True:
        session = None
        ws = None

        try:
            print("[Gate WS] Connecting...")

            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None))
            ws = await session.ws_connect(
                GATE_WS, heartbeat=20, autoping=True,
                headers={"X-Gate-Size-Decimal": "1"})

            _gate_ws_ref["ws"] = ws
            _gate_subscribed_symbols = set()
            _ws_status['gate']['connected'] = True
            print("[Gate WS] Connected!")

            await _gate_subscribe_pending(ws, session)
            last_sync = time.time()

            while True:
                now = time.time()
                if now - last_sync > 15:
                    await _gate_subscribe_pending(ws, session)
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

                    if data.get('channel') != GATE_LIQ_CHANNEL:
                        continue

                    event = data.get('event')
                    if event in ('subscribe', 'unsubscribe'):
                        err = data.get('error')
                        if err:
                            print(f"[Gate WS] sub err: {err}")
                            # Ошибочные подписки повторим позже
                            _gate_subscribed_symbols = set()
                        else:
                            print("[Gate WS] Sub OK")
                        continue

                    if event not in ('update', 'all', None):
                        continue

                    parsed = await _parse_gate_ws_events(
                        session, data)
                    if not parsed:
                        continue

                    async with _gate_ws_lock:
                        _gate_ws_events.extend(parsed)
                        cutoff = time.time() - _WS_BUFFER_TTL
                        _gate_ws_events = [
                            e for e in _gate_ws_events
                            if e['time'] >= cutoff
                        ]

                    _ws_status['gate']['events'] += len(parsed)
                    _ws_status['gate']['last_event_ts'] = time.time()

                    for ev in parsed:
                        if ev['usd_value'] >= 50000:
                            print(
                                f"[Gate WS] "
                                f"{ev['symbol']} "
                                f"{ev['direction']} "
                                f"${ev['usd_value']:,.0f}")

                elif msg.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR):
                    print("[Gate WS] Lost")
                    break

        except asyncio.CancelledError:
            print("[Gate WS] остановлен")
            raise
        except Exception as e:
            print(f"[Gate WS] err: {e}")
        finally:
            _ws_status['gate']['connected'] = False
            _gate_ws_ref["ws"] = None
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

        print("[Gate WS] Reconnect 5s...")
        await asyncio.sleep(5)


def ws_health() -> dict:
    """Короткая диагностика WS-источников для статуса/логов."""
    now = time.time()
    out = {}
    sizes = {
        'bybit': len(_bybit_ws_events),
        'binance': len(_binance_ws_events),
        'gate': len(_gate_ws_events),
    }
    for name, st in _ws_status.items():
        last = st.get('last_event_ts', 0)
        out[name] = {
            'connected': bool(st.get('connected')),
            'events_total': int(st.get('events', 0)),
            'buffered': sizes.get(name, 0),
            'age_sec': (now - last) if last else None,
        }
    return out


# ==================================================
#                 АГРЕГАТОР
# ==================================================

async def get_all_liquidations(
        session, base_symbol, min_usd=10000):
    base = base_symbol.replace('_USDT', '')

    # Подстраховка: если монету не зарегистрировали заранее через
    # set_symbols(), WS-источники всё равно на неё подпишутся.
    ensure_symbol(base_symbol)

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
        _get_binance_lsr(session, f"{base}USDT"),
        return_exceptions=True,
    )

    lsr = {}
    values = []
    labels = ['Gate.io', 'Bybit', 'OKX', 'Binance']

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
        _get_binance_funding(session, f"{base}USDT"),
        return_exceptions=True,
    )

    funding = {}
    values = []
    labels = ['Gate.io', 'Bybit', 'OKX', 'Binance']

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
        _get_binance_oi_change(
            session, f"{base}USDT"),
        return_exceptions=True,
    )

    changes = {}
    values = []
    labels = ['Gate.io', 'Bybit', 'OKX', 'Binance']

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
