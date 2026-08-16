
_bybit_last_plan_signature = None
_BYBIT_SUPPORTED_TTL = 21600

# Binance WS (публичный стрим ликвидаций)
# !forceOrder@arr — все маркет-ликвидации по всем символам без авторизации.
_binance_ws_events = []
_binance_ws_lock = asyncio.Lock()

# Gate.io WS (публичный стрим ликвидаций)
# futures.order_book_liquidates на wss://api.gateio.ws/ws/v4/futures.usdt
_gate_ws_events = []
_gate_ws_lock = asyncio.Lock()
_gate_subscribed_symbols = set()
_gate_last_subscribe_ts = 0.0

GATE_BASE = "https://api.gateio.ws/api/v4/futures/usdt"
OKX_BASE = "https://www.okx.com/api/v5/public"
BYBIT_REST_BASE = "https://api.bybit.com/v5/market"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
BINANCE_FAPI = "https://fapi.binance.com"  # USDT-M Futures forceOrders endpoint
BINANCE_WS = "wss://fstream.binance.com/ws/!forceOrder@arr"
GATE_WS = "wss://api.gateio.ws/ws/v4/futures.usdt"


# ============== ХЕЛПЕРЫ ==============

async def get_gate_liquidations(
        session, contract, min_usd=10000):
    """Ликвидации с Gate.io USDT-M Futures.

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
    Источник: WebSocket wss://api.gateio.ws/ws/v4/futures.usdt +
    канал futures.order_book_liquidates (публичный, не требует auth).

        size = abs(o.get('size', 0))
        order_size = o.get('order_size', 0)
        fill_price = float(o.get('fill_price', 0))
    REST-эндпоинт /liq_orders в текущих версиях API возвращает пустой
    ответ без аутентификации, поэтому в версии 2.x был заменён на
    WS-подход. Буфер событий наполняется в gate_ws_listener().

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
    Формат события WS:
    {
      "channel": "futures.order_book_liquidates",
      "event": "all",
      "time": 1700000000,
      "payload": [
        {"contract": "BTC_USDT", "size": -10, "price": 50000, "time": 1700000000}
      ]
    }
    size < 0 → forced SELL → liquidation of LONG → direction "LONG"
    size > 0 → forced BUY  → liquidation of SHORT → direction "SHORT"
    """
    contract_norm = contract.replace('-', '_').upper()
    last_ts = _last_seen['gate'].get(contract_norm, 0)

        if ts > max_ts:
            max_ts = ts
    # Основной путь — WS-буфер
    async with _gate_ws_lock:
        events = [
            e for e in _gate_ws_events
            if e['symbol'] == contract_norm
            and e['usd_value'] >= min_usd
            and e['time'] > last_ts
        ]

    if max_ts > last_ts:
        _last_seen['gate'][contract] = max_ts
    if events:
        _last_seen['gate'][contract_norm] = max(e['time'] for e in events)

    return events

        session, base_symbol, min_usd=10000):
    """Ликвидации с Binance USDT-M Futures.

    Источник: GET https://fapi.binance.com/fapi/v1/forceOrders
    Возвращает только USDT-M (linear) контракты. Без API ключей,
    но endpoint публичный. Binance — крупнейшая биржа по объёмам,
    у них самые большие ликвидационные каскады, поэтому подключение
    этой биржи критично для детекции крупных каскадов.
    Источник: WebSocket wss://fstream.binance.com/ws/!forceOrder@arr
    (публичный стрим, не требует auth). Буфер событий наполняется
    в binance_ws_listener() и читается здесь.

    Поддерживает lookback только до 7 дней, после чего Binance
    очищает историю. Дельта-фильтрация через _last_seen['binance']
    работает корректно даже после перезапуска бота: при первом
    запуске last_ts=0 и бот получит все ликвидации за последние
    100 записей — обычно это последние минуты/часы.
    REST-эндпоинт /fapi/v1/forceOrders требует HMAC-подпись
    (USER_DATA), поэтому в версии 2.x был заменён на WS-подход,
    который работает без API-ключей.

    Формат forceOrders (документация Binance):
    Формат сообщения WS (документация Binance):
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
      "e":"forceOrder",
      "E": 1700000000000,   # ms epoch
      "o": {
        "s":"BTCUSDT",
        "S":"SELL" | "BUY",
        "q":"0.001",
        "p":"50000.00",
        "ap":"50000.00",
        "X":"FILLED",
        "l":"0.001",
        "z":"0.001",
        "T":1700000000000
      }
    }
    Сторона "BUY" = ликвидация SHORT позиции (forced buyback)
    Сторона "SELL" = ликвидация LONG позиции (forced sell)
    Поэтому в нашей логике:
      side SELL → liquidated_long → direction "LONG"
      side BUY  → liquidated_short → direction "SHORT"
    SELL = закрытие LONG → direction "LONG"
    BUY  = закрытие SHORT → direction "SHORT"
    """
    symbol = base_symbol.replace('_USDT', '') + 'USDT'
    last_ts = _last_seen['binance'].get(base_symbol, 0)

    url = f"{BINANCE_FAPI}/fapi/v1/forceOrders"
    data = await fetch_json(
        session, url,
        params={'symbol': symbol, 'limit': 100}
    )
    # WS-буфер наполняется в binance_ws_listener. Если буфер пуст
    # (например, WS ещё не подключился) — возвращаем пустой список.
    async with _binance_ws_lock:
        events = [
            e for e in _binance_ws_events
            if e['symbol'] == base_symbol
            and e['usd_value'] >= min_usd
            and e['time'] > last_ts
        ]

    if not data or not isinstance(data, list):
        return []
    if events:
        _last_seen['binance'][base_symbol] = max(e['time'] for e in events)

    events = []
    max_ts = last_ts
    return events

    for o in data:
        # Binance отдаёт time в миллисекундах
        ts_ms = o.get('time', 0)
        if not ts_ms:
            continue
        ts = float(ts_ms) / 1000.0
        # Дельта-фильтрация (time в секундах, last_ts в секундах)
        if ts <= last_ts:
            continue

# ==================================================
#                 BINANCE WS
# ==================================================

def _parse_binance_ws_event(data):
    """Парсит одно сообщение из Binance WS-стрима !forceOrder@arr."""
    try:
        if not isinstance(data, dict):
            return None
        if data.get('e') != 'forceOrder':
            return None
        o = data.get('o') or {}
        sym = o.get('s')
        side = o.get('S')
        if not sym or not side:
            return None
        # Количество — берём "z" (кумулятивное исполненное) или "l" (последнее)
        qty = 0.0
        for k in ('z', 'l', 'q'):
            v = o.get(k)
            if v is None:
                continue
            try:
                qty = float(v)
                if qty > 0:
                    break
            except (TypeError, ValueError):
                continue
        # Цена — берём "ap" (средняя) или "p" (цена)
        price = 0.0
        for k in ('ap', 'p'):
            v = o.get(k)
            if v is None:
                continue
            try:
                price = float(v)
                if price > 0:
                    break
            except (TypeError, ValueError):
                continue
        if qty <= 0 or price <= 0:
            return None
        # Время — "E" (event time, ms) или "T" (trade time, ms)
        ts_ms = data.get('E') or o.get('T') or 0
        try:
            price = float(o.get('avgPrice') or o.get('price') or 0)
            qty = float(o.get('executedQty') or o.get('qty') or 0)
            ts = float(ts_ms) / 1000.0 if ts_ms else time.time()
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
            ts = time.time()
        # Нормализуем символ: BTCUSDT -> BTC_USDT
        sym_n = sym
        if sym_n.endswith('USDT') and '_' not in sym_n:
            sym_n = sym_n[:-4] + '_USDT'
        direction = 'LONG' if str(side).upper() == 'SELL' else 'SHORT'
        return {
            'exchange': 'Binance',
            'symbol': base_symbol,
            'symbol': sym_n,
            'time': ts,
            'direction': direction,
            'usd_value': usd,
            'usd_value': qty * price,
            'price': price,
        })
        }
    except Exception:
        return None

        if ts > max_ts:
            max_ts = ts

    if max_ts > last_ts:
        _last_seen['binance'][base_symbol] = max_ts
async def binance_ws_listener():
    """Слушает публичный WS Binance !forceOrder@arr и наполняет _binance_ws_events.

    return events
    Стрим даёт ВСЕ ликвидации по ВСЕМ символам — фильтрация по нужным
    монетам делается в get_binance_liquidations по полю 'symbol'.
    """
    while True:
        session = None
        ws = None
        try:
            print("[Binance WS] Connecting...")
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None)
            )
            ws = await session.ws_connect(
                BINANCE_WS, heartbeat=20, autoping=True
            )
            print("[Binance WS] Connected!")

            while True:
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=5)
                except asyncio.TimeoutError:
                    continue

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except Exception:
                        continue
                    ev = _parse_binance_ws_event(data)
                    if ev:
                        async with _binance_ws_lock:
                            _binance_ws_events.append(ev)
                            cutoff = time.time() - 600
                            _binance_ws_events = [
                                e for e in _binance_ws_events
                                if e['time'] >= cutoff
                            ]
                elif msg.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR):
                    print("[Binance WS] Lost")
                    break
        except asyncio.CancelledError:
            print("[Binance WS] остановлен")
            raise
        except Exception as e:
            print(f"[Binance WS] err: {e}")
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

        print("[Binance WS] Reconnect 5s...")
        await asyncio.sleep(5)


# ==================================================
#                 GATE.IO WS
# ==================================================

def _parse_gate_ws_event(data, symbols_set):
    """Парсит событие ликвидации из WS Gate.io.

    Канал futures.order_book_liquidates на wss://api.gateio.ws/ws/v4/futures.usdt.
    Событие приходит как:
    {
      "channel": "futures.order_book_liquidates",
      "event": "all" | "update",
      "time": 1700000000,
      "payload": [
        {"contract": "BTC_USDT", "size": -10, "price": 50000, "time": 1700000000}
      ]
    }
    size < 0 → forced SELL (liquidation of LONG) → direction "LONG"
    size > 0 → forced BUY (liquidation of SHORT) → direction "SHORT"
    """
    try:
        if not isinstance(data, dict):
            return None
        channel = data.get('channel', '')
        if 'liquidate' not in str(channel).lower():
            return None
        payload = data.get('payload')
        if not isinstance(payload, list):
            return None
        out = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            contract = item.get('contract') or item.get('symbol')
            if not contract:
                continue
            contract = str(contract).upper().replace('-', '_')
            # Фильтруем по нужным символам сразу — экономим память.
            if symbols_set and contract not in symbols_set:
                continue
            try:
                size = float(item.get('size', 0))
            except (TypeError, ValueError):
                size = 0
            try:
                price = float(item.get('price', 0))
            except (TypeError, ValueError):
                price = 0
            if size == 0 or price == 0:
                continue
            # Время — time в секундах или ms
            ts_raw = item.get('time') or item.get('ts') or data.get('time') or 0
            try:
                ts = float(ts_raw)
                if ts > 10_000_000_000:  # это ms
                    ts = ts / 1000.0
            except (TypeError, ValueError):
                ts = time.time()
            usd = abs(size) * price  # Gate.io contracts: size in contracts, ctVal=1 обычно
            if size < 0:
                direction = 'LONG'   # forced SELL → liquidation of LONG
            else:
                direction = 'SHORT'  # forced BUY  → liquidation of SHORT
            out.append({
                'exchange': 'Gate.io',
                'symbol': contract,
                'time': ts,
                'direction': direction,
                'usd_value': usd,
                'price': price,
            })
        return out
    except Exception:
        return []


def set_gate_symbols(symbols):
    """Обновляет список символов для подписки Gate.io WS."""
    global _gate_subscribed_symbols
    _gate_subscribed_symbols = set()
    for s in (symbols or []):
        if not s:
            continue
        norm = str(s).strip().upper().replace('-', '_')
        _gate_subscribed_symbols.add(norm)


async def gate_ws_listener():
    """Слушает публичный WS Gate.io с подпиской на ликвидации.

    Подключается к wss://api.gateio.ws/ws/v4/futures.usdt, подписывается
    на канал futures.order_book_liquidates для выбранных символов.
    События складываются в _gate_ws_events.
    """
    while True:
        session = None
        ws = None
        try:
            print("[Gate.io WS] Connecting...")
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None)
            )
            ws = await session.ws_connect(
                GATE_WS, heartbeat=20, autoping=True
            )
            print("[Gate.io WS] Connected!")

            # Подпишемся сразу на нужные символы
            try:
                payload = sorted(list(_gate_subscribed_symbols)) or ["!all"]
                sub_msg = {
                    "time": int(time.time()),
                    "channel": "futures.order_book_liquidates",
                    "event": "subscribe",
                    "payload": payload,
                }
                await ws.send_json(sub_msg)
                print(f"[Gate.io WS] subscribed: {len(payload)} symbols")
            except Exception as e:
                print(f"[Gate.io WS] subscribe err: {e}")

            last_subscribe = time.time()

            while True:
                now = time.time()
                # Переподписка каждые 30с на случай новых символов
                if now - last_subscribe > 30:
                    try:
                        payload = sorted(list(_gate_subscribed_symbols)) or ["!all"]
                        await ws.send_json({
                            "time": int(now),
                            "channel": "futures.order_book_liquidates",
                            "event": "subscribe",
                            "payload": payload,
                        })
                        last_subscribe = now
                    except Exception as e:
                        print(f"[Gate.io WS] re-subscribe err: {e}")

                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=5)
                except asyncio.TimeoutError:
                    continue

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except Exception:
                        continue
                    # Подтверждение подписки
                    if data.get('event') in ('subscribe', 'unsubscribe'):
                        ch = data.get('channel', '')
                        if 'liquidate' in str(ch).lower():
                            err = data.get('error') or {}
                            if err:
                                print(f"[Gate.io WS] sub err: {err}")
                        continue
                    events = _parse_gate_ws_event(data, _gate_subscribed_symbols)
                    if events:
                        async with _gate_ws_lock:
                            _gate_ws_events.extend(events)
                            cutoff = time.time() - 600
                            _gate_ws_events = [
                                e for e in _gate_ws_events
                                if e['time'] >= cutoff
                            ]
                elif msg.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR):
                    print("[Gate.io WS] Lost")
                    break
        except asyncio.CancelledError:
            print("[Gate.io WS] остановлен")
            raise
        except Exception as e:
            print(f"[Gate.io WS] err: {e}")
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

        print("[Gate.io WS] Reconnect 5s...")
        await asyncio.sleep(5)

# ==================================================
#                 АГРЕГАТОР