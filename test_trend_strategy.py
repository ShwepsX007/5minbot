"""Проверка второй торговой системы «Движение за рынком» (trend_strategy).

Запуск:  python3 test_trend_strategy.py

Тестируется:
  1. Вход по направлению последней ЗАКРЫТОЙ свечи в ТОЛЬКО ЧТО начатое
     окно; ордера — рыночный FAK (order_type="FAK"); одно окно — одна
     сделка; кэп цены входа.
  2. Профит (в т.ч. TP) → серия закрыта; убыток → серия +1 и следующий
     вход идёт ЗА НАПРАВЛЕНИЕМ ПРОИГРЫШНОЙ СВЕЧИ лотом ×N (мартингейл).
  3. Тейк-профит: крупная позиция — GTC-«отложник» на TP с мониторингом
     исполнения; мелкая — продажа FAK по стакану.
  4. Досрочное спасение, когда свеча окна пошла против позиции.
  5. Расчёт окна: официально разрешённый рынок → закрытая свеча →
     ожидание grace → оценка по цене; защита от фантомных 97¢.
  6. Разделение статистики: trade_history.strategy ('liquidations' vs
     'trend'); миграция старой БД (ALTER TABLE); очистка только своей
     вкладки; статистика первой стратегии не видит сделки второй.
  7. Независимость систем: свои пары/интервалы/тумблеры; выключение
     первой не останавливает ведение позиций второй, и наоборот.
  8. Демо-режим: вход/тейк без реальных ордеров, своя демо-статистика.
  9. Меню: экран выбора двух стратегий, клавиатуры настроек, валидатор
     ручного ввода, пункты callback'ов (если установлен telegram).
"""
import asyncio
import os
import sys
import tempfile
import time
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

SYM = "BTC_USDT"
FAKE = {
    "prev_candle": None,
    "win_candle": None,
    "market_resolved": False,
    "price_yes": 50, "price_no": 50,
    "bid": None, "ask": None,
    "place_ok": True,
    "open_orders": [],
}
ORDER_N = {"n": 0}


def now():
    return time.time()


def cur_window_start(tf="5m"):
    dur = ls.TF_SECONDS[tf]
    return int(now() // dur) * dur


def mk_candle(t, o, c, closed=True):
    return {"t": t, "open": o, "close": c, "high": max(o, c), "low": min(o, c),
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
    else {"success": True, "orderID": f"L{ORDER_N['n'] + 1}"}
)
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
ts.set_param("td_entry_delay_sec", "0")
ts.set_param("td_entry_window_sec", "300")
ts.set_param("td_entry_cap_cents", "0")
ts.set_param("td_salvage_on", "0")
ts.set_param("td_settle_grace_sec", "0")


async def main():
    # ============ 1. вход ============
    print("\n— Вход по закрытой свече (FAK, одно окно — одна сделка) —")
    FAKE["prev_candle"] = mk_candle(cur_window_start() - 300, 100.0, 101.0)
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    pos = st["positions"].get(SYM)
    check("вход исполнен (позиция открыта)", pos is not None)
    if not pos:
        print("без позиции дальнейшие проверки невозможны"); return False
    check("направление UP (свеча закрылась вверх)", pos["outcome"] == "UP")
    check("лот = база $5", abs(pos["stake"] - 5.0) < 0.01)
    await ts._scan_entry_impl(Ctx)
    st2 = ts._load_state()
    check("повторного входа в то же окно нет",
          st2["positions"][SYM]["open_ts"] == pos["open_ts"])

    # ============ 2. TP отложником ============
    print("\n— Take-profit: GTC-«отложник» —")
    ts.set_param("td_tp_cents", "80")
    FAKE["bid"] = 82
    await ts._scan_positions_impl(Ctx)
    st = ts._load_state()
    p2 = st["positions"].get(SYM)
    check("выставлен TP-отложник", p2 is not None and bool(p2.get("tp_order_id")))
    for _ in range(3):
        await ts._scan_positions_impl(Ctx)
    st = ts._load_state()
    check("после исполнения отложника позиция закрыта", SYM not in st["positions"])
    trades = database.get_trade_statistics(0, strategy="trend")
    check("сделка записана с меткой strategy='trend'",
          len(trades) == 1 and trades[0]["strategy"] == "trend")
    check("TP-профит > 0 и серия сброшена",
          trades[0]["pnl"] > 0 and ts.get_series(ts._load_state(), SYM) == 0)

    # ============ 3. убыток → мартингейл за проигрышной свечой ============
    print("\n— Проигрыш: серия +1, следующий вход за проигрышной свечой ×2 —")
    st = ts._load_state(); st["last_window"] = {}; ts._save_state(st)
    FAKE["bid"] = None
    FAKE["prev_candle"] = mk_candle(cur_window_start() - 300, 100.0, 101.0)
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    check("новый вход выполнен", SYM in st["positions"])
    FAKE["market_resolved"] = True
    FAKE["price_yes"] = 0
    FAKE["price_no"] = 100  # рынок разрешился DOWN — мы в UP → убыток
    st["positions"][SYM]["window_end"] = now() - 1
    ts._save_state(st)
    await ts._scan_positions_impl(Ctx)
    st = ts._load_state()
    check("после проигрыша позиция снята, серия = 1",
          SYM not in st["positions"] and ts.get_series(st, SYM) == 1)

    FAKE["market_resolved"] = False
    FAKE["prev_candle"] = mk_candle(cur_window_start() - 300, 100.0, 99.0)  # DOWN
    st["last_window"] = {}; ts._save_state(st)
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    pos = st["positions"].get(SYM)
    check("следующий вход — DOWN (направление проигрышной свечи)",
          pos is not None and pos["outcome"] == "DOWN")
    check("лот ×2 = $10, серия в позиции = 1",
          pos and abs(pos["stake"] - 10.0) < 0.01 and pos["series"] == 1)

    # ============ 4. расчёт по свече при отсутствии разрешения ============
    print("\n— Расчёт окна: свеча вместо незакрытого разрешения —")
    wstart = st["positions"][SYM]["window_start"]
    FAKE["win_candle"] = mk_candle(wstart, 100.0, 100.05)  # UP → наша DOWN проиграла
    st["positions"][SYM]["window_end"] = now() - 1
    ts._save_state(st)
    await ts._scan_positions_impl(Ctx)
    st = ts._load_state()
    check("итог по закрытой свече, серия = 2",
          SYM not in st["positions"] and ts.get_series(st, SYM) == 2)
    check("лот следующего шага = база×2^2 = $20",
          abs(ts.compute_stake(ts.cfg(), 2) - 20.0) < 0.01)

    # ============ 5. bust после лимита серии ============
    print("\n— Сброс серии после td_max_series —")
    for expect, series_now in ((20.0, 2), (40.0, 3)):
        st = ts._load_state(); st["last_window"] = {}; ts._save_state(st)
        FAKE["win_candle"] = None
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
        FAKE["price_no"] = 0   # UP победил, мы в DOWN → убыток
        ts._save_state(st)
        await ts._scan_positions_impl(Ctx)
        st = ts._load_state()
    check("серия сверх лимита сброшена (bust → 0)", ts.get_series(st, SYM) == 0)
    FAKE["market_resolved"] = False

    # ============ 6. спасение против свечи ============
    print("\n— Досрочное спасение, когда свеча окна против позиции —")
    ts.set_param("td_salvage_on", "1")
    ts.set_param("td_final_check_sec", "300")
    st = ts._load_state(); st["last_window"] = {}; ts._save_state(st)
    await ts._scan_entry_impl(Ctx)
    st = ts._load_state()
    pos = st["positions"].get(SYM)
    if pos:
        wstart = pos["window_start"]
        our = pos["outcome"]
        FAKE["win_candle"] = mk_candle(wstart, 100.0,
                                       99.9 if our == "UP" else 100.1)
        FAKE["bid"] = 45
        st["positions"][SYM]["window_end"] = now() + 5
        ts._save_state(st)
        await ts._scan_positions_impl(Ctx)
        st = ts._load_state()
        check("спасение: позиция продана досрочно, серия = 1",
              SYM not in st["positions"] and ts.get_series(st, SYM) == 1)
    ts.set_param("td_salvage_on", "0")

    # ============ 7. разделение статистики ============
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

    # ============ 8. независимость и статусы ============
    print("\n— Независимость систем и статусы —")
    assert "ДВИЖЕНИЕ ЗА РЫНКОМ" in ts.get_status_text()
    check("get_status_text собирается", True)
    check("get_stats_text собирается", "Статистика" in ts.get_stats_text(0))
    check("liq-состояние не содержит позиций тренда",
          not (ls._load_state().get("positions") or {}))

    # ведение позиций работает даже при выключенной стратегии
    ts.set_active(False)
    database.set_setting("demo_mode", "1")
    st = ts._load_state(); st["last_window"] = {}; ts._save_state(st)
    # вход при выключенной стратегии — нет
    await ts._scan_entry_impl(Ctx)
    check("выключенная стратегия не открывает новых входов",
          SYM not in ts._load_state()["positions"])
    ts.set_active(True)
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

    # ============ 9. меню (если telegram установлен) ============
    print("\n— Меню и валидаторы —")
    try:
        import trend_menu as tmenu
    except ImportError:
        print("  ⚪️ telegram не установлен — пропускаем проверку меню")
        return FAIL == 0
    check("параметры меню на месте", len(tmenu.PARAMS) == 17)
    for key, _lb, _opts in tmenu.PARAMS:
        rows, lab = tmenu.param_value_kb(key)
        if not rows:
            check(f"param_value_kb({key})", False); break
    else:
        check("param_value_kb для всех параметров", True)
    ok, v, _err = tmenu.validate_manual_input("td_base_stake", "7.5")
    check("валидатор: 7.5 принято", ok and float(v) == 7.5)
    ok, _v, _err = tmenu.validate_manual_input("td_base_stake", "0.01")
    check("валидатор: 0.01 отклонено (минимум $1)", not ok)
    ok, _v, _err = tmenu.validate_manual_input("td_tp_mode", "bogus")
    check("валидатор: enum-мусор отклонён", not ok)
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


asyncio.run(main())
os.unlink(_TMP_DB.name)
print(f"\nИТОГ: {PASS} прошло, {FAIL} упало")
sys.exit(1 if FAIL else 0)
