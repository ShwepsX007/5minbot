"""Проверка второй торговой системы «Движение за рынком» (trend_strategy) v2.

Запуск:  python3 test_trend_strategy.py

Тестируется:
  1. Основной вход: за N секунд до КОНЦА текущего окна бот смотрит его
     ЖИВУЮ свечу и входит в СЛЕДУЮЩЕЕ окно по направлению свечи (пока
     цена ~50¢). Ордера — рыночный FAK; одно окно — одна сделка; кэп цены.
  2. Фолбэк: если граница упущена (нет свечи) — вход в первые секунды
     уже начавшегося окна по последней ЗАКРЫТОЙ свече; дубль подавляется
     меткой bd_seen.
  3. При открытой позиции на границе: свеча в нашу сторону — держим;
     против — закрытие рынка (FAK по стакану) и СРАЗУ вход в следующее
     окно в направлении свечи лотом мартингейла.
  4. Профит (в т.ч. TP) → серия закрыта; убыток → серия +1 и следующий
     вход идёт за направлением проигрышной свечи лотом ×N.
  5. Тейк-профит: крупная позиция — GTC-«отложник» с мониторингом;
     мелкая — FAK.
  6. Расчёт окна: официально разрешённый рынок → закрытая свеча →
     ожидание grace → оценка по цене; защита от фантомных 97¢.
  7. Разделение статистики: trade_history.strategy ('liquidations' vs
     'trend'); очистка только своей вкладки.
  8. Независимость систем; ведение позиций при выключенном тумблере;
     демо-режим.
  9. Меню: параметры v2 (td_entry_lead_sec вместо td_final_check_sec),
     валидатор ручного ввода (если установлен telegram).

Время модуля стратегии заморожено (ts.time), чтобы детерминированно
попадать в границу окна.
"""
import asyncio
import os
import sys
import tempfile
import time as _time
import types as _types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Тестовая БД, чтобы не трогать боевую.
import config
_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMP_DB.close()
config.DB_FILE = _TMP_DB.name

import database
database.DB_FILE = _TMP_DB.name
database.init_db()

# Заглушка polymarket_trading: ордера не отправляются, web3-стек не нужен.
_pt = _types.ModuleType("polymarket_trading")
sys.modules["polymarket_trading"] = _pt

import logging
logging.basicConfig(level=logging.CRITICAL)

import chainlink_price as clp
import liq_strategy as ls
import trend_strategy as ts

PASS = 0
FAIL = 0
SENT = []


def check(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} {extra}")


class FakeBot:
    async def send_message(self, cid, text, **kw):
        SENT.append(text)
        return types_ns


types_ns = _types.SimpleNamespace(message_id=1)


class FakeJob:
    data = {"cid": 1}


Ctx = _types.SimpleNamespace(bot=FakeBot(), job=FakeJob())

# ---------- Заморозка часов стратегии ----------
FROZEN = [0.0]
ts.time = _types.SimpleNamespace(time=lambda: FROZEN[0])
try:
    ts.time.monotonic = _time.monotonic   # на всякий случай
except Exception:
    pass

SYM = "BTC_USDT"
TF = "5m"
DUR = ls.TF_SECONDS[TF]
FAKE = {
    "prev_candle": None,     # candle «предыдущего» окна (для фолбэка)
    "win_candle": None,      # candle «текущего» окна (для границы)
    "market_resolved": False,
    "price_yes": 50, "price_no": 50,
    "bid": None, "ask": None,
    "place_ok": True,
    "open_orders": [],
}
ORDER_N = {"n": 0}


def now():
    return FROZEN[0]


def real_now():
    return _time.time()


def window_start(offset_windows=0):
    """Граница окна, отсчитанная от НАСТОЯЩЕГО времени (смещение в окнах)."""
    return int(real_now() // DUR) * DUR + offset_windows * DUR


def freeze_at_boundary(offset_windows=0, lead=1.0):
    """Ставим часы за `lead` секунд до конца окна (offset_windows от реального now).

    Возвращает start окна, граница которого наступает."""
    w = window_start(offset_windows)
    FROZEN[0] = w + DUR - lead
    return w


def freeze_inside_window(offset_windows=0, age=5.0):
    w = window_start(offset_windows)
    FROZEN[0] = w + age
    return w


def mk_candle(t, o, c, closed=True, hi=None, lo=None):
    return {"t": t, "open": o, "close": c,
            "high": hi if hi is not None else max(o, c),
            "low": lo if lo is not None else min(o, c),
            "closed": closed, "src": "chainlink_twap60"}


# ---------- заглушки торговой инфраструктуры (вешаем на stub-модуль) ----------
async def fake_get_candles(session, symbol, timeframe="5m", limit=5, force=False):
    out = []
    if FAKE["prev_candle"]:
        out.append(FAKE["prev_candle"])
    if FAKE["win_candle"]:
        out.append(FAKE["win_candle"])
    return out


def fake_clp_window_candle(symbol, ws, we, tf="5m"):
    if FAKE["prev_candle"] and int(FAKE["prev_candle"]["t"]) == int(ws):
        return FAKE["prev_candle"]
    if FAKE["win_candle"] and int(FAKE["win_candle"]["t"]) == int(ws):
        return FAKE["win_candle"]
    return None


_pt.get_event_markets = lambda slug: {"title": slug, "markets": [{
    "question": "Bitcoin Up or Down", "token_yes": "TY", "token_no": "TN",
    "price_yes": FAKE["price_yes"], "price_no": FAKE["price_no"],
    "active": True, "neg_risk": False, "accepting_orders": True,
    "resolved": FAKE["market_resolved"],
}]}
_pt.get_market_info = lambda tid: {"neg_risk": False, "accepting_orders": True,
                                   "closed": False, "min_size": 5.0,
                                   "min_shares": 5.0, "tick_size": 0.01}


def fake_place_market_order(token_id, side, amount, order_type="FOK"):
    assert order_type == "FAK", f"ожидался FAK, получен {order_type}"
    if not FAKE["place_ok"]:
        return {"success": False, "errorMsg": "book too thin"}
    ORDER_N["n"] += 1
    price = ((FAKE["ask"] or FAKE["bid"] or 50) if side == "BUY"
             else (FAKE["bid"] or 50)) / 100.0
    shares = round(float(amount) / price, 4) if side == "BUY" else float(amount)
    cost = round(shares * price, 4)
    return {"success": True, "orderID": f"M{ORDER_N['n']}", "status": "matched",
            "sizeMatched": shares,
            "takingAmount": cost, "makingAmount": cost}


_pt.place_market_order = fake_place_market_order
_pt.place_order = lambda token_id, side, price, size, allow_min_bump=True: (
    {"success": False, "errorMsg": "nope"} if not FAKE["place_ok"]
    else {"success": True, "orderID": f"L{ORDER_N['n'] + 1}"})
_pt.is_order_accepted = lambda res: (
    (False, str(res.get("errorMsg"))) if isinstance(res, dict) and res.get("success") is False
    else (False, str(res.get("error"))) if isinstance(res, dict) and res.get("error")
    else (True, "ok"))


def fake_extract_fill(res, side="BUY"):
    if not isinstance(res, dict) or "sizeMatched" not in res:
        return None
    return {"shares": res["sizeMatched"], "price": None,
            "cost": res.get("takingAmount"), "status": res.get("status")}


_pt._extract_fill = fake_extract_fill
_pt.get_balance = lambda: 1000.0
_pt.get_live_price = lambda tid: None if (FAKE["bid"] is None and FAKE["ask"] is None) \
    else {"bid": FAKE["bid"], "ask": FAKE["ask"], "mid": None}
_pt.cancel_order = lambda oid: {"success": True}
_pt.get_open_orders = lambda: list(FAKE["open_orders"])
_pt._extract_order_id = lambda o: (o.get("orderID") if isinstance(o, dict) else "") or ""
_pt.is_ready = lambda: True
_pt._client = None
_pt._object_to_dict = lambda o: o if isinstance(o, dict) else {}

clp.get_window_candle = fake_clp_window_candle
ls.get_candles = fake_get_candles

# ---------- конфигурация второй стратегии ----------
ts.set_selected_symbols([SYM])
ts.set_active(True)
ts.set_param("td_base_stake", "5")
ts.set_param("td_martingale_mult", "2")
ts.set_param("td_max_series", "3")
ts.set_param("td_entry_lead_sec", "2")
ts.set_param("td_check_interval", "1")
ts.set_param("td_entry_delay_sec", "0")
ts.set_param("td_entry_window_sec", "60")
ts.set_param("td_entry_cap_cents", "0")
ts.set_param("td_salvage_on", "0")
ts.set_param("td_settle_grace_sec", "0")


def hard_reset():
    """Полная чистка состояния стратегии (позиции, серии, метки окон)."""
    ts.reset_state()
    st = ts._load_state()
    for k in ("positions", "series", "series_pnl", "last_window", "try_cnt",
              "bd_seen"):
        st[k] = {}
    ts._save_state(st)


async def main():
    # ============ 1. вход на границе окна ============
    print("\n— Вход за 1с до конца окна в СЛЕДУЮЩЕЕ окно (FAK) —")
    hard_reset()
    w = freeze_at_boundary(lead=1.0)          # 1 сек до конца окна w
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)   # живая свеча «в плюс»
    FAKE["prev_candle"] = None
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    pos = st["positions"].get(SYM)
    check("граница: вход исполнен", pos is not None)
    if not pos:
        print("без позиции дальнейшие проверки невозможны"); return False
    check("направление UP (текущая свеча в плюс)", pos["outcome"] == "UP")
    check("цель — СЛЕДУЮЩЕЕ окно", pos["window_start"] == w + DUR
          and pos["window_end"] == w + 2 * DUR)
    check("лот = база $5, серия 0", abs(pos["stake"] - 5.0) < 0.01
          and pos["series"] == 0)
    check("вход по FAK (entry ~50¢, догонять нечего)",
          45 <= pos["entry_cents"] <= 55)
    check("метка границы стоит", int(st["bd_seen"].get(SYM, -1)) == w)
    before = pos["open_ts"]
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    check("повторного входа на ту же границу нет (bd_seen)",
          st["positions"][SYM]["open_ts"] == before)

    # ============ 2. свеча в минус → DOWN следующего окна ============
    print("\n— Граница: свеча в минус → вход DOWN —")
    hard_reset()
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 99.0)
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    pos = st["positions"].get(SYM)
    check("вход DOWN следующего окна", pos is not None and pos["outcome"] == "DOWN"
          and pos["window_start"] == w + DUR)

    # ============ 3. кэп цены входа ============
    print("\n— Кэп цены: улетевшее окно пропускаем —")
    hard_reset()
    ts.set_param("td_entry_cap_cents", "70")
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)
    FAKE["ask"] = 75            # наш токен стоит 75¢ — дорого
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    check("цена выше кэпа — входа нет", SYM not in st["positions"])
    check("окно сожжено (не будет и фолбэка)",
          int(st["last_window"].get(SYM, -1)) == w + DUR)
    FAKE["ask"] = None
    ts.set_param("td_entry_cap_cents", "0")

    # ============ 4. фолбэк по закрытой свече ============
    print("\n— Фолбэк: граница упущена → вход в начало окна по закрытой свече —")
    hard_reset()
    w2 = freeze_inside_window(age=5.0)          # 5с от старта окна w2
    cur = window_start()
    check("мы реально внутри только что начавшегося окна",
          abs(w2 - cur) < 2 and 0 < now() - w2 < DUR - 3 - ts._boundary_lead(ts.cfg()))
    FAKE["win_candle"] = None                   # текущей свечи ещё нет
    FAKE["prev_candle"] = mk_candle(w2 - DUR, 100.0, 99.2, closed=True)  # DOWN
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    pos = st["positions"].get(SYM)
    check("фолбэк-вход DOWN в текущее окно",
          pos is not None and pos["outcome"] == "DOWN"
          and pos["window_start"] == w2)
    # Дубль: граница предыдущего окна уже всё решила — фолбэк молчит
    hard_reset()
    st = ts._load_state(); st["bd_seen"][SYM] = w2 - DUR; ts._save_state(st)
    await ts._scan_entry_impl(Ctx)
    check("после решения на границе фолбэк не дублирует вход",
          SYM not in ts._load_state()["positions"])
    # Вне полос (середина окна) — вход не ищется
    hard_reset()
    freeze_inside_window(age=DUR / 2)
    FAKE["prev_candle"] = mk_candle(window_start() - DUR, 100.0, 99.0)
    await ts._scan_entry_impl(Ctx)
    check("в середине окна бот спит", SYM not in ts._load_state()["positions"])

    # ============ 5. удержание при попутной свече ============
    print("\n— Позиция + попутная свеча на границе → держим —")
    hard_reset()
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)
    await ts._scan_entry_impl(Ctx)          # вход UP в окно w+DUR
    st = ts._load_state()
    pos = st["positions"].get(SYM)
    check("позиция открыта (предыстория)", pos is not None)
    # теперь ГРАНИЦА окна w+DUR: её свеча тоже UP — держим, не пересаживаемся
    w = freeze_at_boundary(1, lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 101.0, 102.0)
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    pos2 = st["positions"].get(SYM)
    check("попутная свеча — позиция держится, новых нет",
          pos2 is not None and pos2["open_ts"] == pos["open_ts"]
          and len(st["positions"]) == 1)
    check("в следующее окно НЕ зашли",
          int(st["last_window"].get(SYM, -1)) != w + DUR)

    # ============ 6. спасение на границе + мгновенный вход за свечой ============
    print("\n— Свеча против на границе: закрыть рынок и СРАЗУ в следующее окно —")
    hard_reset()
    ts.set_param("td_salvage_on", "1")
    # вход UP в окно w+DUR (на границе w)
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    pos = st["positions"].get(SYM)
    check("предыстория: позиция UP открыта", pos is not None)
    # граница окна w+DUR: его свеча DOWN (против нас), bid=45¢
    w = freeze_at_boundary(1, lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 102.0, 101.0)   # DOWN
    FAKE["bid"] = 45
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    pos2 = st["positions"].get(SYM)
    check("серия стала 1 (убыток закрытия)", ts.get_series(st, SYM) == 1)
    check("СРАЗУ открыта новая позиция DOWN",
          pos2 is not None and pos2["outcome"] == "DOWN")
    if pos2:
        check("новая позиция — в следующее окно (w+2·DUR)",
              pos2["window_start"] == w + DUR)
        check("лот мартингейла ×2 = $10, серия 1",
              abs(pos2["stake"] - 10.0) < 0.01 and pos2["series"] == 1)
    trades = database.get_trade_statistics(0, strategy="trend")
    closed = [t for t in trades if float(t.get("pnl", 0)) < 0]
    check("закрытая позиция записана как убыток (продажа по 45¢)",
          any(t["close_price"] == 45 for t in closed))
    FAKE["bid"] = None
    ts.set_param("td_salvage_on", "0")

    # ============ 7. TP отложником ============
    print("\n— Take-profit: GTC-«отложник» —")
    hard_reset()
    ts.set_param("td_tp_cents", "80")
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    check("вход для TP-теста", SYM in st["positions"])
    FAKE["bid"] = 82
    await ts._scan_positions_impl(Ctx)
    st = ts._load_state()
    p2 = st["positions"].get(SYM)
    check("выставлен TP-отложник", p2 is not None and bool(p2.get("tp_order_id")))
    # ордер «исчез из открытых» 3 проверки подряд = считаем исполненным
    for _ in range(3):
        await ts._scan_positions_impl(Ctx)
    st = ts._load_state()
    check("после исполнения отложника позиция закрыта", SYM not in st["positions"])
    FAKE["open_orders"] = []
    trades = database.get_trade_statistics(0, strategy="trend")
    tp_tr = [t for t in trades if int(t.get("close_price") or 0) == 80]
    check("TP-сделка записана со стратегией trend и ценой 80¢",
          bool(tp_tr) and tp_tr[0]["strategy"] == "trend")
    check("TP-профит > 0 и серия сброшена",
          tp_tr and float(tp_tr[0]["pnl"]) > 0
          and ts.get_series(ts._load_state(), SYM) == 0)
    FAKE["bid"] = None

    # ============ 8. убыток по расчёту → серия ×2, направление за свечой ============
    print("\n— Расчёт окна: проигрыш → серия +1, следующий вход ×2 —")
    st = ts._load_state()
    hard_reset()
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)      # вход UP
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    check("новый вход выполнен", SYM in st["positions"])
    FAKE["market_resolved"] = True
    FAKE["price_yes"] = 0
    FAKE["price_no"] = 100          # рынок разрешился DOWN — мы в UP → убыток
    st["positions"][SYM]["window_end"] = now() - 1
    FROZEN[0] = FROZEN[0] + 1
    ts._save_state(st)
    await ts._scan_positions_impl(Ctx)
    st = ts._load_state()
    check("после проигрыша позиция снята, серия = 1",
          SYM not in st["positions"] and ts.get_series(st, SYM) == 1)
    FAKE["market_resolved"] = False
    # граница следующего окна: свеча DOWN → вход DOWN лотом $10
    w = freeze_at_boundary(1, lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 101.0, 99.5)
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    pos = st["positions"].get(SYM)
    check("следующий вход — DOWN (направление проигрышной свечи)",
          pos is not None and pos["outcome"] == "DOWN")
    check("лот ×2 = $10, серия в позиции = 1",
          pos and abs(pos["stake"] - 10.0) < 0.01 and pos["series"] == 1)

    # ============ 9. расчёт по свече при отсутствии разрешения ============
    print("\n— Расчёт окна: свеча вместо незакрытого разрешения —")
    wstart = st["positions"][SYM]["window_start"]
    FAKE["win_candle"] = mk_candle(wstart, 100.0, 100.05)  # UP → наша DOWN проиграла
    st = ts._load_state()
    st["positions"][SYM]["window_end"] = now() - 1
    FROZEN[0] = FROZEN[0] + 1
    ts._save_state(st)
    await ts._scan_positions_impl(Ctx)
    st = ts._load_state()
    check("итог по закрытой свече, серия = 2",
          SYM not in st["positions"] and ts.get_series(st, SYM) == 2)
    check("лот следующего шага = база×2^2 = $20",
          abs(ts.compute_stake(ts.cfg(), 2) - 20.0) < 0.01)

    # ============ 10. bust после лимита серии ============
    print("\n— Сброс серии после td_max_series —")
    for expect, series_now in ((20.0, 2), (40.0, 3)):
        w = freeze_at_boundary(series_now + 1, lead=1.0)
        st = ts._load_state(); st["last_window"] = {}; st["bd_seen"] = {}
        ts._save_state(st)
        FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)
        await ts._scan_entry_impl(Ctx)
        st = ts._load_state()
        p = st["positions"].get(SYM)
        check(f"вход шага {series_now}+1 с лотом ${expect:g}",
              p is not None and abs(p["stake"] - expect) < 0.01)
        if not p:
            break
        st["positions"][SYM]["window_end"] = now() - 1
        FAKE["market_resolved"] = True
        FAKE["price_yes"] = 100
        FAKE["price_no"] = 0       # UP победил, мы в… любом — фиксируем убыток
        # направление входа чередуем так, чтобы шаг проигрывал:
        if p["outcome"] == "DOWN":
            FAKE["price_yes"], FAKE["price_no"] = 100, 0
        else:
            FAKE["price_yes"], FAKE["price_no"] = 0, 100
        ts._save_state(st)
        FROZEN[0] = FROZEN[0] + 1
        await ts._scan_positions_impl(Ctx)
        st = ts._load_state()
    check("серия сверх лимита сброшена (bust → 0)", ts.get_series(st, SYM) == 0)
    FAKE["market_resolved"] = False

    # ============ 10.5 фильтр флета: сигнальное окно vs предыдущая свеча ============
    print("\n— 🌫 Флет: лика/CVD/OI против prev окна —")
    import liq_api as lqa_t
    import orderflow as ofl_t

    # --- утилиты отношения ---
    check("ratio: 100/1000 = 10%", ts._cmp_ratio(100.0, 1000.0) == 10.0)
    check("ratio: 0/0 = 0 (оба тихие = флет)", ts._cmp_ratio(0.0, 0.0) == 0.0)
    check("ratio: 5/0 = ∞", ts._cmp_ratio(5.0, 0.0) == float("inf"))
    check("ratio: нет данных → None", ts._cmp_ratio(None, 1.0) is None)
    check("below: 10 < 60 → блок", ts._ratio_blocks(10.0, 60.0, "below"))
    check("below: 70 ≥ 60 → пропуск", not ts._ratio_blocks(70.0, 60.0, "below"))
    check("above: 200 > 150 → блок", ts._ratio_blocks(200.0, 150.0, "above"))
    check("нет данных → никогда не блокируем", not ts._ratio_blocks(None, 60.0, "below"))

    # --- ликовикция: движение угасло → вход блокируется ---
    def liq_events(events):
        async def _re(symbol, min_usd=1000.0, since=0.0):
            return [e for e in events if e["time"] >= since]
        return _re

    hard_reset()
    ts.set_param("td_liq_prev_pct", "60")
    ts.set_param("td_liq_prev_mode", "below")
    lqa_t.ws_liquid_ready = lambda: True
    w = freeze_at_boundary(lead=1.0)
    sig_end = w + DUR
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)     # UP-свеча есть
    lqa_t.recent_liquidations = liq_events([
        {"time": w - DUR + 30, "usd_value": 500.0},      # prev окно: $500
        {"time": w - 40, "usd_value": 500.0},             # prev окно: $500
        {"time": w + 60, "usd_value": 100.0},              # текущее: $100
    ])                                                     # ratio 100/1000=10% <60
    stat0 = int(float(database.get_setting("td_stat_flat_liq", "0") or 0))
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    pos_r = st["positions"].get(SYM)
    check("лика: 10% < 60% → РЕВЕРС: вход DOWN (против UP-свечи)",
          pos_r is not None and pos_r["outcome"] == "DOWN")
    check("лика: целевое окно отмечено входом (не горит дважды)",
          int(st["last_window"].get(SYM, -1)) == w + DUR
          and pos_r is not None and pos_r["window_start"] == w + DUR)
    stat1 = int(float(database.get_setting("td_stat_flat_liq", "0") or 0))
    check("лика: счётчик фильтра вырос", stat1 == stat0 + 1)
    check("лика: реверс описан в td_last_scan",
          "реверс" in (database.get_setting("td_last_scan", "") or ""))
    check("лика: в сообщении — заголовок реверса",
          any("ВХОД ПРОТИВ СВЕЧИ" in m for m in SENT))

    # --- то же отношение, но порог пройден → вход есть ---
    hard_reset()
    lqa_t.recent_liquidations = liq_events([
        {"time": w - DUR + 30, "usd_value": 100.0},       # prev: $100
        {"time": w + 60, "usd_value": 700.0},              # cur: $700 → 700%
    ])
    await ts._scan_entry_impl(Ctx)
    p2 = ts._load_state()["positions"].get(SYM)
    check("лика: 700% ≥ 60% → обычный вход по свече (UP)",
          p2 is not None and p2["outcome"] == "UP")

    # --- оба окна пустые, но WS жив: подтверждённый флет ---
    hard_reset()
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)
    lqa_t.recent_liquidations = liq_events([])
    await ts._scan_entry_impl(Ctx)
    p3 = ts._load_state()["positions"].get(SYM)
    check("никаких ликвидаций 2 окна подряд → реверс DOWN (0% < 60%)",
          p3 is not None and p3["outcome"] == "DOWN")

    # --- WS мёртв → данных нет → не блокируем ---
    hard_reset()
    lqa_t.ws_liquid_ready = lambda: False
    await ts._scan_entry_impl(Ctx)
    lqa_t.ws_liquid_ready = lambda: True
    p4 = ts._load_state()["positions"].get(SYM)
    check("WS ликвидаций лежит → фильтр молчит, вход как по свече (UP)",
          p4 is not None and p4["outcome"] == "UP")
    ts.set_param("td_liq_prev_pct", "0")

    # --- CVD: поток выдохся ---
    hard_reset()
    ts.set_param("td_cvd_prev_pct", "50")
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)
    ofl_t.status = lambda: {"connected": True, "age_sec": 1, "trades": 999,
                            "symbols": 1, "last_error": ""}
    def fake_flow(symbol, t0, t1):
        if t0 == w:
            return {"cvd": 5000.0, "trades": 120}     # текущее: слабый поток
        if t0 == w - DUR:
            return {"cvd": -40000.0, "trades": 300}   # prev: сильный
        return None
    ofl_t.flow_stats = fake_flow
    await ts._scan_entry_impl(Ctx)
    p5 = ts._load_state()["positions"].get(SYM)
    check("CVD 12.5% < 50% (поток умер) → реверс DOWN",
          p5 is not None and p5["outcome"] == "DOWN")
    stat_c = int(float(database.get_setting("td_stat_flat_cvd", "0") or 0))
    check("CVD: счётчик вырос", stat_c >= 1)

    hard_reset()
    def fake_flow2(symbol, t0, t1):
        if t0 == w:
            return {"cvd": -30000.0, "trades": 250}   # поток продолжается
        if t0 == w - DUR:
            return {"cvd": -40000.0, "trades": 300}
        return None
    ofl_t.flow_stats = fake_flow2
    await ts._scan_entry_impl(Ctx)
    p6 = ts._load_state()["positions"].get(SYM)
    check("CVD 75% ≥ 50% → вход по свече (UP)",
          p6 is not None and p6["outcome"] == "UP")

    # нет данных потока → не мешает
    hard_reset()
    ofl_t.status = lambda: {"connected": False, "age_sec": None, "trades": 0,
                            "symbols": 0, "last_error": "reconnecting"}
    await ts._scan_entry_impl(Ctx)
    p7 = ts._load_state()["positions"].get(SYM)
    check("CVD-стрим лежит → вход не реверсится",
          p7 is not None and p7["outcome"] == "UP")
    ts.set_param("td_cvd_prev_pct", "0")
    ofl_t.flow_stats = lambda symbol, t0, t1: None

    # --- OI: снапшоты границ ---
    hard_reset()
    ts.set_param("td_oi_prev_pct", "50")
    oi_calls = {"n": 0}
    async def fake_oi(session, symbol):
        oi_calls["n"] += 1
        return {"average": -0.2, "Gate.io": -0.2}
    lqa_t.get_multi_oi_change = fake_oi
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)
    await ts._scan_entry_impl(Ctx)          # снапшот на границу w
    st = ts._load_state()
    check("OI: снапшот границы записан", str(int(w)) in (st.get("oi_snap", {}).get(SYM) or {}))
    check("OI: сеть дёрнута ровно 1 раз", oi_calls["n"] == 1)
    p8 = ts._load_state()["positions"].get(SYM)
    check("OI: prev-снапшота нет → не реверсим (первая граница)",
          p8 is not None and p8["outcome"] == "UP")
    # следующая граница: свежий снапшот 0.01% против 0.2% → 5% < 50% → блок
    hard_reset()
    st = ts._load_state()
    st["oi_snap"][SYM] = {str(int(w)): 0.2}    # prev окно
    ts._save_state(st)
    w2 = freeze_at_boundary(1, lead=1.0)        # граница следующего окна
    FAKE["win_candle"] = mk_candle(w2, 101.0, 102.0)
    async def fake_oi_weak(session, symbol):
        return {"average": -0.01}
    lqa_t.get_multi_oi_change = fake_oi_weak
    await ts._scan_entry_impl(Ctx)
    p9 = ts._load_state()["positions"].get(SYM)
    check("OI 5% < 50% → реверс DOWN (нет новых позиций = флет)",
          p9 is not None and p9["outcome"] == "DOWN")
    st = ts._load_state()
    check("OI: снапшот новой границы добавлен", str(int(w2)) in (st.get("oi_snap", {}).get(SYM) or {}))
    ts.set_param("td_oi_prev_pct", "0")

    # --- режим above: перегрев блокируется ---
    hard_reset()
    ts.set_param("td_liq_prev_pct", "150")
    ts.set_param("td_liq_prev_mode", "above")
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)
    lqa_t.recent_liquidations = liq_events([
        {"time": w - DUR + 10, "usd_value": 100.0},
        {"time": w + 10, "usd_value": 3000.0},       # 3000% > 150%
    ])
    await ts._scan_entry_impl(Ctx)
    pa = ts._load_state()["positions"].get(SYM)
    check("above: шторм 3000% > 150% → реверс DOWN",
          pa is not None and pa["outcome"] == "DOWN")

    # --- независимость: у каждой метрики СВОЙ режим above/below ---
    hard_reset()
    ts.set_param("td_prev_mode", "below")
    ts.set_param("td_liq_prev_pct", "150")
    ts.set_param("td_liq_prev_mode", "above")     # лика: блок только на всплеске
    ts.set_param("td_cvd_prev_pct", "50")
    ts.set_param("td_cvd_prev_mode", "below")     # cvd: блок на затухании
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)
    lqa_t.recent_liquidations = liq_events([
        {"time": w - DUR + 10, "usd_value": 100.0},
        {"time": w + 10, "usd_value": 100.0},          # 100% < 150 → лика пропускает
    ])
    ofl_t.status = lambda: {"connected": True, "age_sec": 1, "trades": 9,
                            "symbols": 1, "last_error": ""}
    def flow_weak(symbol, t0, t1):
        if t0 == w:
            return {"cvd": 4000.0, "trades": 90}       # 10% < 50 → cvd блокирует
        if t0 == w - DUR:
            return {"cvd": 40000.0, "trades": 200}
        return None
    ofl_t.flow_stats = flow_weak
    stat_c0 = int(float(database.get_setting("td_stat_flat_cvd", "0") or 0))
    await ts._scan_entry_impl(Ctx)
    pb = ts._load_state()["positions"].get(SYM)
    check("лика(>150) и cvd(<50) не мешают друг другу: реверсит cvd",
          pb is not None and pb["outcome"] == "DOWN"
          and int(float(database.get_setting("td_stat_flat_cvd", "0") or 0))
          == stat_c0 + 1)

    hard_reset()
    def flow_strong(symbol, t0, t1):
        if t0 == w:
            return {"cvd": 39000.0, "trades": 90}      # 97.5% — cvd пропускает
        if t0 == w - DUR:
            return {"cvd": 40000.0, "trades": 200}
        return None
    ofl_t.flow_stats = flow_strong
    lqa_t.recent_liquidations = liq_events([
        {"time": w - DUR + 10, "usd_value": 100.0},
        {"time": w + 10, "usd_value": 2500.0},         # 2500% > 150 → блокирует лика
    ])
    stat_l0 = int(float(database.get_setting("td_stat_flat_liq", "0") or 0))
    await ts._scan_entry_impl(Ctx)
    pc = ts._load_state()["positions"].get(SYM)
    check("та же пара фильтров на «всплеске»: реверсит лика, а не cvd",
          pc is not None and pc["outcome"] == "DOWN"
          and int(float(database.get_setting("td_stat_flat_liq", "0") or 0))
          == stat_l0 + 1)
    ofl_t.flow_stats = lambda symbol, t0, t1: None

    # --- старый ОБЩИЙ td_prev_mode работает как fallback ---
    hard_reset()
    ts.set_param("td_liq_prev_pct", "150")
    database.set_setting("td_liq_prev_mode", "")       # отдельный режим не задан
    database.set_setting("td_prev_mode", "above")      # старая общая настройка
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)
    lqa_t.recent_liquidations = liq_events([
        {"time": w - DUR + 10, "usd_value": 100.0},
        {"time": w + 10, "usd_value": 2500.0},         # 2500% — блок только в above
    ])
    await ts._scan_entry_impl(Ctx)
    pd_ = ts._load_state()["positions"].get(SYM)
    check("наследие: пустой режим → берётся старый общий td_prev_mode "
          "(реверс DOWN)",
          pd_ is not None and pd_["outcome"] == "DOWN")

    # и наоборот: ratio < порога в унаследованном above НЕ блокирует
    hard_reset()
    lqa_t.recent_liquidations = liq_events([
        {"time": w - DUR + 10, "usd_value": 100.0},
        {"time": w + 10, "usd_value": 50.0},            # 50% — below бы заблокировал
    ])
    await ts._scan_entry_impl(Ctx)
    pe = ts._load_state()["positions"].get(SYM)
    check("унаследованный above: 50% < 150% → реверса нет, вход UP",
          pe is not None and pe["outcome"] == "UP")

    # --- реверс действует и для шагов серии мартингейла ---
    hard_reset()
    st = ts._load_state()
    st["series"][SYM] = 1
    ts._save_state(st)
    ts.set_param("td_liq_prev_pct", "60")
    ts.set_param("td_liq_prev_mode", "below")
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)   # свеча UP
    lqa_t.recent_liquidations = liq_events([
        {"time": w - DUR + 10, "usd_value": 1000.0},
        {"time": w + 60, "usd_value": 100.0},           # 10% < 60 → реверс
    ])
    await ts._scan_entry_impl(Ctx)
    pm = ts._load_state()["positions"].get(SYM)
    check("шаг серии: сработка → лот ×2 в ОБРАТНУЮ сторону",
          pm is not None and pm["outcome"] == "DOWN"
          and abs(pm["stake"] - 10.0) < 0.01 and pm["series"] == 1)

    database.set_setting("td_prev_mode", "")
    ts.set_param("td_liq_prev_mode", "below")
    ts.set_param("td_liq_prev_pct", "0")
    ts.set_param("td_cvd_prev_mode", "below")
    ts.set_param("td_cvd_prev_pct", "0")
    ts.set_param("td_oi_prev_pct", "0")
    lqa_t.recent_liquidations = liq_events([])
    lqa_t.ws_liquid_ready = lambda: True

    # --- union подписок WS (чтобы trend-монеты не выписывал скан liq) ---
    check("flow_symbols() = объединение монет обеих стратегий",
          SYM in ls.flow_symbols())
    # фильтры выключены → никакого влияния на обычный путь
    hard_reset()
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)
    await ts._scan_entry_impl(Ctx)
    pf = ts._load_state()["positions"].get(SYM)
    check("все пороги 0 → обычный вход без изменений",
          pf is not None and pf["outcome"] == "UP")

    # ============ 11. разделение статистики ============
    print("\n— Статистика: разделы не пересекаются —")
    database.add_trade_history(0, "btc-updown-5m-111", "Bitcoin Up or Down",
                               "UP", "BUY", 5, 50, 100, 2.5,
                               strategy="liquidations")
    database.add_trade_history(0, "manual-market", "Manual", "YES", "BUY",
                               1, 40, 60, 1.0)
    trend_tr = database.get_trade_statistics(0, strategy="trend")
    liq_tr = database.get_trade_statistics(0, strategy="liquidations")
    all_tr = database.get_trade_statistics(0)
    check("вкладка trend видит только свои сделки",
          trend_tr and all(t["strategy"] == "trend" for t in trend_tr))
    check("вкладка liquidations не видит сделки trend",
          all(t.get("strategy") != "trend" for t in liq_tr))
    check("«все» = trend + liquidations + ручная",
          len(all_tr) == len(trend_tr) + len(liq_tr) + 1)
    lst = ts.get_trade_stats(0)
    lls = ls._get_trade_stats(0)
    check("сводка второй стратегии = свои сделки", lst["total"] == len(trend_tr))
    check("сводка ликвидаций не считает сделки второй стратегии",
          all(str(t.get("strategy") or "").lower() != "trend" for t in lls["recent"]))
    ts.clear_stats(0)
    check("очистка статистики 2-й стратегии не тронула 1-ю",
          len(database.get_trade_statistics(0, strategy="liquidations")) == len(liq_tr)
          and len(database.get_trade_statistics(0, strategy="trend")) == 0)

    # ============ 12. независимость, демо, статус ============
    print("\n— Независимость систем, демо и статусы —")
    status = ts.get_status_text()
    check("get_status_text: есть lead-строка", "до конца окна" in status)
    check("get_status_text: нет следа старого final_check",
          "final_check" not in status and "Спасение: вкл за" not in status)
    check("get_stats_text собирается", "Статистика" in ts.get_stats_text(0))
    check("liq-состояние не содержит позиций тренда",
          not (ls._load_state().get("positions") or {}))

    ts.set_active(False)
    hard_reset()
    database.set_setting("demo_mode", "1")
    w = freeze_at_boundary(lead=1.0)
    FAKE["win_candle"] = mk_candle(w, 100.0, 101.0)
    await ts._scan_entry_impl(Ctx)
    check("выключенная стратегия не открывает новых входов",
          SYM not in ts._load_state()["positions"])
    ts.set_active(True)
    w = freeze_at_boundary(lead=1.0)
    await ts._scan_entry_impl(Ctx)          # демо-вход
    st = ts._load_state()
    check("демо-вход открыт", SYM in st["positions"]
          and st["positions"][SYM]["is_demo"] == 1)
    ts.set_active(False)                    # «выключили» — позиция всё равно доживается
    FAKE["bid"] = 82
    await ts._scan_positions_impl(Ctx)
    check("ведение позиции работает и при выключенном тумблере",
          SYM not in ts._load_state()["positions"])
    check("демо-сделка попала только в демо-статистику",
          len(database.get_trade_statistics(1, strategy="trend")) == 1
          and len(database.get_trade_statistics(0, strategy="trend")) == 0)
    ts.set_active(True)
    database.set_setting("demo_mode", "0")
    FAKE["bid"] = None

    # ============ 13. меню (если telegram установлен) ============
    print("\n— Меню и валидаторы —")
    try:
        import trend_menu as tmenu
    except ImportError:
        print("  ⚪️ telegram не установлен — пропускаем проверку меню")
        return FAIL == 0
    keys = [k for k, _, _ in tmenu.PARAMS]
    check("в меню есть td_entry_lead_sec", "td_entry_lead_sec" in keys)
    check("из меню убран td_final_check_sec", "td_final_check_sec" not in keys)
    for key, _lb, _opts in tmenu.PARAMS:
        rows, lab = tmenu.param_value_kb(key)
        if not rows:
            check(f"param_value_kb({key})", False); break
    else:
        check("param_value_kb для всех параметров", True)
    ok, v, _err = tmenu.validate_manual_input("td_entry_lead_sec", "3")
    check("валидатор: lead=3 принято", ok and str(v) == "3")
    ok, _v, err = tmenu.validate_manual_input("td_entry_lead_sec", "0")
    check("валидатор: lead=0 отклонено (мин. 1с)", not ok)
    ok, v, _err = tmenu.validate_manual_input("td_base_stake", "7.5")
    check("валидатор: 7.5 принято", ok and float(v) == 7.5)
    ok, _v, _err = tmenu.validate_manual_input("td_base_stake", "0.01")
    check("валидатор: 0.01 отклонено (минимум $1)", not ok)
    ok, _v, _err = tmenu.validate_manual_input("td_tp_mode", "bogus")
    check("валидатор: enum-мусор отклонён", not ok)
    check("дефолты стратегии: lead есть, final_check нет",
          "td_entry_lead_sec" in ts.DEFAULTS
          and "td_final_check_sec" not in ts.DEFAULTS)
    for fk in ("td_liq_prev_pct", "td_liq_prev_mode", "td_cvd_prev_pct",
               "td_cvd_prev_mode", "td_oi_prev_pct", "td_oi_prev_mode"):
        if fk not in keys:
            check(f"в меню есть {fk}", False); break
    else:
        check("в меню все 6 параметров флета (порог+режим на метрику)", True)
    check("старый общий td_prev_mode убран из меню и дефолтов",
          "td_prev_mode" not in keys and "td_prev_mode" not in ts.DEFAULTS)
    ok, v, _e = tmenu.validate_manual_input("td_liq_prev_pct", "75")
    check("валидатор: liq_pct=75 принято", ok and str(v) == "75")
    ok, _v, _e = tmenu.validate_manual_input("td_liq_prev_pct", "-1")
    check("валидатор: отрицательный порог отклонён", not ok)
    _st = (ts.set_param("td_cvd_prev_pct", "50"), ts.get_status_text())
    check("статус: строка фильтра с пометкой реверса",
          "против.prev" in _st[1].lower()
          and "вход в обратную сторону" in _st[1])
    ts.set_param("td_cvd_prev_pct", "0")
    try:
        import bot
        kb = bot.strategies_kb()
        labels = " ".join(b.text for row in kb.inline_keyboard for b in row)
        check("экран выбора: две стратегии",
              "Каскад ликвидаций" in labels and "Движение за рынком" in labels)
        check("фоновые задачи двух стратегий зарегистрированы в именах",
              {"liq_signal_job", "liq_position_job",
               "trend_signal_job", "trend_position_job"} <= set(bot.STRATEGY_JOB_NAMES))
    except ImportError as e:
        print(f"  ⚪️ bot не импортирован в тестовой среде ({e})")
    return True


ok = asyncio.run(main())
os.unlink(_TMP_DB.name)
print(f"\nИТОГ: {PASS} прошло, {FAIL} упало")
sys.exit(0 if (ok and FAIL == 0) else 1)
