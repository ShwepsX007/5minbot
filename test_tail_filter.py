"""Проверка фильтра хвоста ликвидаций и связанных настроек.

Запуск:  python3 test_tail_filter.py

Тестируется:
  1. eval_tail_filter — доля хвоста считается по объёму ($) за последние
     liq_tail_sec секунд от всего окна liq_window_sec; режимы above/below;
     выключенный фильтр; пустой буфер.
  2. check_entry_filters — фильтр хвоста блокирует первичный вход и
     попадает в details.
  3. _mg_window_confirmed — подтверждение шага мартингейла теперь ходит
     через тот же фильтр хвоста.
  4. liq_menu — новые параметры в меню, валидация ручного ввода.
  5. Пауза сигналов после отмены входа фильтром (раздел 8).
  6. Свеча окна — из того же 60-секундного TWAP-потока, по которому
     Polymarket рассчитывает рынки 5m/15m/1h (раздел 9).
  7. FAK-тейк отката по первой ставке: гейт по шагу/времени окна и
     демо-исполнение (раздел 10).
"""
import asyncio
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Тестовая БД, чтобы не трогать боевую.
import config
_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMP_DB.close()
config.DB_FILE = _TMP_DB.name

import database
database.DB_FILE = _TMP_DB.name
database.init_db()

# Заглушка polymarket_trading: в тестах ордера не отправляются, а модуль
# тянет web3-стек (eth_account), который для проверки логики не нужен.
import types as _types
sys.modules.setdefault("polymarket_trading", _types.ModuleType("polymarket_trading"))

import liq_strategy as ls
import liq_menu

PASS = 0
FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} {extra}")


def make_events(total_usd_window: float, tail_usd: float, window_sec: int,
                tail_sec: int, now: float) -> list:
    """Синтетические ликвидации: равномерный фон по всему окну + заданный
    объём в хвосте (последние tail_sec секунд)."""
    events = []
    body_usd = total_usd_window - tail_usd
    if body_usd > 0:
        # фон: 10 событий равномерно по «телу» окна
        for i in range(10):
            t = now - window_sec + (window_sec - tail_sec) * (i + 0.5) / 10
            events.append({
                "time": t, "usd_value": body_usd / 10, "direction": "LONG",
                "exchange": "Binance", "symbol": "BTC_USDT", "price": 50000,
            })
    if tail_usd > 0:
        # хвост: 5 событий в последних tail_sec секундах
        for i in range(5):
            t = now - tail_sec + tail_sec * (i + 0.5) / 5
            events.append({
                "time": t, "usd_value": tail_usd / 5, "direction": "SHORT",
                "exchange": "Bybit", "symbol": "BTC_USDT", "price": 50000,
            })
    return events


def set_cfg(**kv):
    for k, v in kv.items():
        ls.set_param(k, v)


import contextlib


@contextlib.asynccontextmanager
async def aiohttp_stub_session():
    """Заглушка вместо aiohttp.ClientSession: фильтр хвоста работает по
    локальному буферу событий и в сеть не ходит."""
    yield None


async def main():
    now = time.time()
    sym = "BTC_USDT"

    print("\n=== 1. eval_tail_filter: базовые сценарии (окно 300с, хвост 50с) ===")
    set_cfg(liq_window_sec="300", liq_tail_sec="50", liq_tail_pct="50",
            liq_tail_mode="above")

    # 1a. режим above: хвост 60% > 50% → блок
    ls._events_buffer[sym] = make_events(100_000, 60_000, 300, 50, now)
    r = ls.eval_tail_filter(sym, now)
    check("above: хвост 60% > 50% → блок", r["blocked"], r["why"])
    check("above: доля посчитана верно", abs(r["share_pct"] - 60.0) < 0.5,
          f"share={r['share_pct']}")

    # 1b. режим above: хвост 40% < 50% → проход
    ls._events_buffer[sym] = make_events(100_000, 40_000, 300, 50, now)
    r = ls.eval_tail_filter(sym, now)
    check("above: хвост 40% < 50% → проход", not r["blocked"], r["why"])

    # 1c. режим below: хвост 40% < 50% → блок
    set_cfg(liq_tail_mode="below")
    r = ls.eval_tail_filter(sym, now)
    check("below: хвост 40% < 50% → блок", r["blocked"], r["why"])

    # 1d. режим below: хвост 60% > 50% → проход
    ls._events_buffer[sym] = make_events(100_000, 60_000, 300, 50, now)
    r = ls.eval_tail_filter(sym, now)
    check("below: хвост 60% > 50% → проход", not r["blocked"], r["why"])

    # 1e. события вне окна не учитываются
    set_cfg(liq_tail_mode="above")
    old = make_events(100_000, 60_000, 300, 50, now - 1000)  # очень старые
    ls._events_buffer[sym] = old
    r = ls.eval_tail_filter(sym, now)
    check("старые события вне окна не считаются",
          r["window_usd"] == 0 and not r["blocked"], f"win={r['window_usd']}")

    # 1f. пустой буфер: above — проход, below — блок
    ls._events_buffer[sym] = []
    r = ls.eval_tail_filter(sym, now)
    check("пустой буфер + above → проход", not r["blocked"])
    set_cfg(liq_tail_mode="below")
    r = ls.eval_tail_filter(sym, now)
    check("пустой буфер + below → блок (каскад затух)", r["blocked"])

    # 1g. фильтр выключен (tail_sec=0)
    set_cfg(liq_tail_sec="0")
    r = ls.eval_tail_filter(sym, now)
    check("tail_sec=0 → фильтр выключен", not r["enabled"] and not r["blocked"])

    # 1h. хвост длиннее окна → прижимается к окну (доля 100%)
    set_cfg(liq_tail_sec="500", liq_tail_mode="above", liq_window_sec="300")
    ls._events_buffer[sym] = make_events(100_000, 60_000, 300, 50, now)
    r = ls.eval_tail_filter(sym, now)
    check("хвост>окно → прижат к окну, доля 100%",
          r["tail_sec"] == 300 and abs(r["share_pct"] - 100.0) < 0.5,
          f"tail={r['tail_sec']} share={r['share_pct']}")
    set_cfg(liq_window_sec="300", liq_tail_sec="50")

    print("\n=== 2. check_entry_filters: фильтр хвоста блокирует первичный вход ===")
    set_cfg(liq_tail_sec="50", liq_tail_mode="above",
            liq_filter_impulse="0", liq_filter_oi_pct="0", liq_filter_cvd="0")
    ls._events_buffer[sym] = make_events(100_000, 80_000, 300, 50, now)
    ok, reasons, details = await ls.check_entry_filters(
        None, sym, "UP", "5m", now - 300, now)
    check("вход заблокирован фильтром хвоста", not ok, str(reasons))
    check("причина упоминает хвост", any("хвост" in r for r in reasons),
          str(reasons))
    check("детали хвоста в details", "tail" in details and
          details["tail"]["share_pct"] > 79, str(details.get("tail")))

    ls._events_buffer[sym] = make_events(100_000, 20_000, 300, 50, now)
    ok, reasons, details = await ls.check_entry_filters(
        None, sym, "UP", "5m", now - 300, now)
    check("хвост в норме → фильтры пропускают", ok, str(reasons))

    print("\n=== 3. _mg_window_confirmed: повторный вход через тот же фильтр ===")
    # убыточная свеча с «плохим» хвостом → окно не подтверждено
    ls._events_buffer[sym] = make_events(100_000, 80_000, 300, 50, now)
    confirmed, why = await ls._mg_window_confirmed(None, sym, "5m", "UP", now - 300)
    check("плохой хвост → шаг не подтверждён", not confirmed, why)

    # хвост в норме → подтверждено
    ls._events_buffer[sym] = make_events(100_000, 20_000, 300, 50, now)
    confirmed, why = await ls._mg_window_confirmed(None, sym, "5m", "UP", now - 300)
    check("хвост в норме → шаг подтверждён", confirmed, why)

    # фильтр выключен → подтверждено по умолчанию
    set_cfg(liq_tail_sec="0")
    confirmed, why = await ls._mg_window_confirmed(None, sym, "5m", "UP", now - 300)
    check("фильтр выключен → подтверждено по умолчанию", confirmed, why)

    print("\n=== 4. меню настроек ===")
    keys = [k for k, _, _ in liq_menu.PARAMS]
    for k in ("liq_tail_sec", "liq_tail_pct", "liq_tail_mode"):
        check(f"{k} есть в меню", k in keys)
        check(f"{k} есть в DEFAULTS", k in ls.DEFAULTS)
        check(f"{k} есть в LIQ_PARAM_META", k in liq_menu.LIQ_PARAM_META)

    ok, norm, err = liq_menu.validate_manual_input("liq_tail_mode", "выше")
    check("ручной ввод «выше» → above", ok and norm == "above", err)
    ok, norm, err = liq_menu.validate_manual_input("liq_tail_mode", "ниже")
    check("ручной ввод «ниже» → below", ok and norm == "below", err)
    ok, norm, err = liq_menu.validate_manual_input("liq_tail_mode", "above")
    check("ручной ввод «above»", ok and norm == "above", err)
    ok, norm, err = liq_menu.validate_manual_input("liq_tail_sec", "50")
    check("ручной ввод 50 сек", ok and norm == "50", err)
    ok, norm, err = liq_menu.validate_manual_input("liq_tail_sec", "9999")
    check("слишком много секунд → ошибка", not ok)
    ok, norm, err = liq_menu.validate_manual_input("liq_tail_pct", "50")
    check("ручной ввод порога 50%", ok and norm == "50.0", err)

    print("\n=== 5. статус/воронка не падают с новыми ключами ===")
    try:
        ls._funnel_line()
        ls._filters_status_line()
        check("funnel/status-line собираются", True)
    except Exception as e:
        check("funnel/status-line собираются", False, str(e))

    print("\n=== 6. интеграция: убыток → оценка убыточной свечи ===")

    class FakeBot:
        async def send_message(self, *a, **k):
            return None

    class FakeContext:
        def __init__(self):
            self.bot = FakeBot()

    ctx = FakeContext()
    entered = []

    class _FakeTime:
        """Подменяет часы: синтетические события в тесте «из будущего»,
        а фильтр хвоста по умолчанию берёт time.time()."""
        def __init__(self, now):
            self._now = now

        def time(self):
            return self._now

    real_time_mod = ls.time

    async def fake_enter(context, cid, c_, state_, symbol_, stake, series_, tf_,
                         carried_outcome=None, entry_note=""):
        entered.append({"series": series_, "stake": stake, "note": entry_note})

    real_enter = ls._enter_martingale_step
    ls._enter_martingale_step = fake_enter

    def fresh_pos(sym, series=0):
        return {
            "slug": f"btc-updown-5m-{int(time.time() // 300) * 300}",
            "token_id": "tok", "outcome": "UP", "stake": 1.0, "shares": 2.0,
            "entry_cents": 51, "entry_mode": "market", "limit_price_cents": 51,
            "min_shares": 5.0, "window_end": time.time() + 300,
            "window_start": int(time.time() // 300) * 300,
            "series": series, "is_demo": 1, "agg_snapshot": {}, "candle": "DOWN",
            "symbol": sym, "open_ts": time.time(), "oi_snapshot": {},
            "market_question_raw": "Bitcoin Up or Down",
        }

    async def settle_loss():
        state = ls._load_state()
        state["positions"] = {sym: fresh_pos(sym)}
        state["series"] = {sym: 0}
        state.setdefault("series_pnl", {}).pop(sym, None)
        state.setdefault("mg_pending", {}).pop(sym, None)
        ls._save_state(state)
        await ls._settle_position(ctx, 1, ls.cfg(), state, sym, fresh_pos(sym),
                                  win=False, close_price=0, price_yes=5, price_no=95,
                                  market_question="Bitcoin Up or Down",
                                  early_exit=True, settle_ts=time.time())

    try:
        set_cfg(liq_tail_sec="50", liq_tail_pct="50", liq_tail_mode="above",
                liq_mg_skip_windows="2", liq_mg_skip_lot_pct="50")

        # 6a. убыточная свеча ПРОШЛА фильтр → вход в следующее окно СРАЗУ
        ls._events_buffer[sym] = make_events(100_000, 20_000, 300, 50, time.time())
        await settle_loss()
        st = ls._load_state()
        check("хвост прошёл → серия продолжается (шаг 1)",
              int((st.get("series") or {}).get(sym, 0)) == 1, str(st.get("series")))
        check("хвост прошёл → план ожидания НЕ создаётся",
              sym not in (st.get("mg_pending") or {}))
        check("хвост прошёл → сразу вход в следующее окно",
              len(entered) == 1 and entered[0]["series"] == 1, str(entered))
        check("вход помечен причиной фильтра хвоста",
              "хвост" in (entered[0]["note"] if entered else ""), str(entered))
        entered.clear()

        # 6b. убыточная свеча ЗАБЛОКИРОВАНА (лимит 2) → план ожидания, пропусков 1/2
        ls._events_buffer[sym] = make_events(100_000, 80_000, 300, 50, time.time())
        await settle_loss()
        st = ls._load_state()
        mg = (st.get("mg_pending") or {}).get(sym) or {}
        check("блок → серия продолжается (шаг 1)",
              int((st.get("series") or {}).get(sym, 0)) == 1, str(st.get("series")))
        check("блок → план ожидания с пропуском 1/2",
              mg.get("skips_used") == 1 and mg.get("max_skip") == 2, str(mg))
        check("блок → сразу входа нет", len(entered) == 0)

        # 6c. блок + лимит пропусков 1 + лот 0% → серия останавливается сразу
        set_cfg(liq_mg_skip_windows="1", liq_mg_skip_lot_pct="0")
        ls._events_buffer[sym] = make_events(100_000, 80_000, 300, 50, time.time())
        await settle_loss()
        st = ls._load_state()
        check("блок + лимит 1 + лот 0% → серия остановлена",
              int((st.get("series") or {}).get(sym, 0)) == 0 and
              sym not in (st.get("mg_pending") or {}) and len(entered) == 0,
              str(st.get("series")))

        # 6d. блок + лимит 0 + лот 60% → сразу вход уменьшенным лотом
        set_cfg(liq_mg_skip_windows="0", liq_mg_skip_lot_pct="60")
        ls._events_buffer[sym] = make_events(100_000, 80_000, 300, 50, time.time())
        await settle_loss()
        st = ls._load_state()
        full_stake, _ = ls.compute_stake(ls.cfg(), st, sym, 1)
        check("блок + лот после пропусков → сразу вход уменьшенным лотом",
              len(entered) == 1 and
              abs(entered[0]["stake"] - round(full_stake * 0.6, 2)) < 0.02,
              f"entered={entered} full={full_stake}")
        entered.clear()
        set_cfg(liq_mg_skip_windows="2", liq_mg_skip_lot_pct="50")

        print("\n=== 7. интеграция: ожидание окна после блока (пропуски и вход) ===")
        # граница 5-минутного окна, за 2с до конца — пора проверять фильтр
        end = (int(time.time()) // 300 + 1) * 300
        now = end - 2

        # Свежий блок на убыточной свече → план ожидания (пропусков 1/2)
        ls._events_buffer[sym] = make_events(100_000, 80_000, 300, 50, time.time())
        await settle_loss()

        # 7a. следующее окно тоже заблокировано → пропуск 2/2, входа нет
        ls._events_buffer[sym] = make_events(100_000, 80_000, 300, 50, now)
        ls.time = _FakeTime(now)
        async with aiohttp_stub_session() as session:
            req = (ls._load_state().get("mg_pending") or {}).get(sym)
            req["created_ts"] = now
            await ls._mg_pending_check_one(ctx, session, ls.cfg(), sym, req, now)
        ls.time = real_time_mod
        st = ls._load_state()
        mg = (st.get("mg_pending") or {}).get(sym) or {}
        check("плохое окно → пропуск 2/2, план на месте",
              mg.get("skips_used") == 2, str(mg))
        check("плохое окно → входа нет", len(entered) == 0)

        # 7b. следующее окно прошло фильтр → шаг входит
        now2 = end + 300 - 2
        ls._events_buffer[sym] = make_events(100_000, 20_000, 300, 50, now2)
        st = ls._load_state()
        req = (st.get("mg_pending") or {}).get(sym)
        req["created_ts"] = now
        ls._save_state(st)
        ls.time = _FakeTime(now2)
        async with aiohttp_stub_session() as session:
            await ls._mg_pending_check_one(ctx, session, ls.cfg(), sym, req, now2)
        ls.time = real_time_mod
        st = ls._load_state()
        check("хорошее окно → план снят", sym not in (st.get("mg_pending") or {}))
        check("хорошее окно → шаг 1 вошёл", len(entered) == 1 and
              entered[0]["series"] == 1, str(entered))
        check("вход помечен причиной фильтра хвоста",
              "хвост" in (entered[0]["note"] if entered else ""), str(entered))
        entered.clear()

        # 7c. лимит пропусков исчерпан, лот 0% → серия останавливается
        st = ls._load_state()
        st["series"] = {sym: 1}
        st["mg_pending"] = {sym: {
            "series": 1, "outcome": "UP", "max_skip": 2, "lot_pct": 0,
            "skips_used": 2, "checked_window": 0, "created_ts": now2,
            "tf": "5m", "cid": 1}}
        ls._save_state(st)
        ls._events_buffer[sym] = make_events(100_000, 80_000, 300, 50, now2)
        ls.time = _FakeTime(now2)
        async with aiohttp_stub_session() as session:
            req = (ls._load_state().get("mg_pending") or {}).get(sym)
            await ls._mg_pending_check_one(ctx, session, ls.cfg(), sym, req, now2)
        ls.time = real_time_mod
        st = ls._load_state()
        check("пропуски исчерпаны + лот 0% → серия остановлена",
              int((st.get("series") or {}).get(sym, 0)) == 0 and
              sym not in (st.get("mg_pending") or {}) and len(entered) == 0)

        # 7d. лимит исчерпан, лот 40% → вход уменьшенным лотом
        st = ls._load_state()
        st["series"] = {sym: 1}
        st["series_pnl"] = {sym: -1.0}
        st["mg_pending"] = {sym: {
            "series": 1, "outcome": "UP", "max_skip": 2, "lot_pct": 40,
            "skips_used": 2, "checked_window": 0, "created_ts": now2,
            "tf": "5m", "cid": 1}}
        ls._save_state(st)
        ls.time = _FakeTime(now2)
        async with aiohttp_stub_session() as session:
            req = (ls._load_state().get("mg_pending") or {}).get(sym)
            await ls._mg_pending_check_one(ctx, session, ls.cfg(), sym, req, now2)
        ls.time = real_time_mod
        st = ls._load_state()
        full_stake, _ = ls.compute_stake(ls.cfg(), st, sym, 1)
        check("пропуски исчерпаны + лот 40% → вход уменьшенным лотом",
              len(entered) == 1 and
              abs(entered[0]["stake"] - round(full_stake * 0.4, 2)) < 0.02,
              f"entered={entered} full={full_stake}")
    finally:
        ls.time = real_time_mod
        ls._enter_martingale_step = real_enter

    print("\n=== 8. пауза сигналов после отмены фильтром ===")
    set_cfg(liq_signal_cooldown_sec="120")
    ls._set_signal_cooldown(sym)
    st = ls._load_state()
    left = ls._signal_cooldown_left(st, sym)
    check("пауза установлена (~120с)", 118 <= left <= 120, f"left={left}")
    check("пауза хранится в state['cooldowns']",
          sym in (st.get("cooldowns") or {}))

    # пауза 0 → отключена, даже если запись осталась
    set_cfg(liq_signal_cooldown_sec="0")
    check("пауза=0 → не действует", ls._signal_cooldown_left(st, sym) == 0)
    st2 = ls._load_state()
    st2["cooldowns"] = {}
    ls._save_state(st2)
    ls._set_signal_cooldown(sym)
    st2 = ls._load_state()
    check("пауза=0 → новая запись не создаётся",
          sym not in (st2.get("cooldowns") or {}))

    # по другой монете паузы нет
    set_cfg(liq_signal_cooldown_sec="120")
    st = ls._load_state()
    check("пауза только по своей монете",
          ls._signal_cooldown_left(st, "ETH_USDT") == 0)

    # сброс состояния чистит паузы
    ls.reset_state()
    st = ls._load_state()
    check("reset_state снимает паузу",
          ls._signal_cooldown_left(st, sym) == 0)

    # меню и дефолты
    keys = [k for k, _, _ in liq_menu.PARAMS]
    check("liq_signal_cooldown_sec есть в меню",
          "liq_signal_cooldown_sec" in keys)
    check("liq_signal_cooldown_sec есть в DEFAULTS",
          "liq_signal_cooldown_sec" in ls.DEFAULTS)
    ok, norm, err = liq_menu.validate_manual_input("liq_signal_cooldown_sec", "120")
    check("ручной ввод паузы 120с", ok and norm == "120", err)
    ok, norm, err = liq_menu.validate_manual_input("liq_signal_cooldown_sec", "9999")
    check("слишком большая пауза → ошибка", not ok)

    print("\n=== 9. свеча окна берётся из того же потока, что и рынок ===")
    import chainlink_price as clp
    # Polymarket считает рынки 5m/15m/1h по 60-секундному TWAP
    # (конфиги btc/eth/sol-5m-twap-60, проверено 27.08.2026). Свеча бота
    # обязана строиться из того же 60-секундного потока, иначе на
    # микродвижениях направление расходится с официальным исходом
    # (27.08.2026 так получился фантомный «плюс» на ETH 18:50–18:55 МСК).
    check("5m → 60-секундный TWAP-поток", clp.twap_window_for("5m") == 60,
          f"получено {clp.twap_window_for('5m')}")
    check("15m → 60-секундный TWAP-поток", clp.twap_window_for("15m") == 60)
    check("1h → 60-секундный TWAP-поток", clp.twap_window_for("1h") == 60)
    check("топик 60с существует",
          clp.TOPIC_BY_WINDOW.get(60) == "crypto_prices_twap_sixty")

    # Сборка свечи: в счёт идут ТОЛЬКО тики 60-секундного потока.
    # Реальный кейс 27.08.2026: ETH 15:50–15:55 UTC, цена старта 2522.460,
    # финальный TWAP 2521.825 → рынок закрылся DOWN.
    real_now = time.time()
    wstart = int(real_now) - 600
    wend = wstart + 300
    sym_cl = "eth/usd"
    clp._ticks[(sym_cl, 60)] = [
        (wstart + 2, 2522.46),     # цена на старте (priceToBeat)
        (wstart + 150, 2522.10),
        (wend - 1, 2521.83),       # финальный TWAP ниже старта → DOWN
    ]
    clp._ticks[(sym_cl, 30)] = [
        (wstart + 2, 2522.46),
        (wend - 1, 2523.00),       # 30-секундный поток показал бы UP —
    ]                              # в свече 5m-рынка он участвовать не должен
    cndl = clp.get_window_candle("ETH_USDT", wstart, wend, "5m")
    check("свеча собирается из 60-секундного потока",
          cndl is not None and cndl.get("src") == "chainlink_twap60",
          f"cndl={cndl}")
    check("close — из 60-секундного потока (тик 30с не попал)",
          cndl is not None and abs(cndl["close"] - 2521.83) < 1e-9,
          f"close={cndl and cndl['close']}")
    check("свеча закрыта", bool(cndl and cndl.get("closed")))
    check("направление DOWN — как официальный итог рынка",
          ls.resolve_state(cndl) == "DOWN",
          f"получено {ls.resolve_state(cndl)}")
    del clp._ticks[(sym_cl, 30)]
    cndl2 = clp.get_window_candle("ETH_USDT", wstart, wend, "5m")
    check("результат не зависит от топика 30с",
          cndl2 is not None and cndl2["close"] == cndl["close"])
    del clp._ticks[(sym_cl, 60)]

    # Официальный расчёт рынка ждём дольше: гамма-АПИ отражает исход
    # с задержкой до ~4 минут (27.08.2026), при 120с бот верил свече.
    check("дефолт ожидания расчёта рынка — 300с",
          ls.DEFAULTS["liq_market_confirm_wait"] == "300",
          ls.DEFAULTS["liq_market_confirm_wait"])

    print("\n=== 10. FAK-тейк отката по первой ставке ===")
    c_full = dict(ls.DEFAULTS)
    tp, sec = ls.get_tp_first_config(c_full)
    check("дефолт: 75¢ / 30с", tp == 75 and sec == 30, f"{tp}/{sec}")
    tp, sec = ls.get_tp_first_config({**c_full, "liq_tp_first_cents": "80",
                                      "liq_tp_first_sec": "45"})
    check("переопределение настройками", tp == 80 and sec == 45)
    tp, sec = ls.get_tp_first_config({**c_full, "liq_tp_first_cents": "abc",
                                      "liq_tp_first_sec": "xyz"})
    check("мусор в настройках → дефолт", tp == 75 and sec == 30)

    now_t = time.time()
    pos1 = {"series": 0, "window_start": now_t - 5, "window_end": now_t + 295,
            "outcome": "UP", "entry_cents": 51, "shares": 10.0, "is_demo": 1,
            "slug": "eth-updown-5m-test"}
    check("первый шаг, 5-я секунда окна → включён",
          ls.first_take_applicable(c_full, pos1, now_t) is True)
    check("шаг мартингейла (series=1) → выключен",
          ls.first_take_applicable(c_full, {**pos1, "series": 1}, now_t) is False)
    check("N секунд окна прошли → выключен",
          ls.first_take_applicable(c_full,
                                   {**pos1, "window_start": now_t - 31},
                                   now_t) is False)
    check("окно ещё не началось → выключен",
          ls.first_take_applicable(c_full,
                                   {**pos1, "window_start": now_t + 10},
                                   now_t) is False)
    check("уровень 0 → выключен",
          ls.first_take_applicable({**c_full, "liq_tp_first_cents": "0"},
                                   pos1, now_t) is False)
    check("окно 0с → выключен",
          ls.first_take_applicable({**c_full, "liq_tp_first_sec": "0"},
                                   pos1, now_t) is False)
    check("awaiting_resolution → выключен",
          ls.first_take_applicable(c_full,
                                   {**pos1, "awaiting_resolution": 1},
                                   now_t) is False)

    # Демо-исполнение: цена выше уровня и выше входа → закрываем в плюс.
    pt_stub = sys.modules["polymarket_trading"]
    real_get_em = getattr(pt_stub, "get_event_markets", None)
    pt_stub.get_event_markets = lambda slug: {"markets": [{
        "question": "Test ETH", "price_yes": 78, "price_no": 22}]}
    settle_calls = []
    real_settle = ls._settle_position

    async def fake_settle(context, cid, c, state, symbol, pos, **kw):
        settle_calls.append({"symbol": symbol, **kw})

    ls._settle_position = fake_settle
    try:
        r = await ls._try_first_take(None, 1, c_full, {}, sym, dict(pos1))
        check("ДЕМО: 78¢ >= 75¢ → закрыли в плюс",
              r is True and len(settle_calls) == 1
              and settle_calls[0]["win"] is True
              and settle_calls[0]["close_price"] == 78)
        check("reason закрытия упоминает FAK-тейк",
              "FAK" in str(settle_calls[0].get("reason", "")))

        pt_stub.get_event_markets = lambda slug: {"markets": [{
            "question": "Test ETH", "price_yes": 60, "price_no": 40}]}
        r = await ls._try_first_take(None, 1, c_full, {}, sym, dict(pos1))
        check("ДЕМО: 60¢ < 75¢ → не закрываем",
              r is False and len(settle_calls) == 1)

        pt_stub.get_event_markets = lambda slug: {"markets": [{
            "question": "Test ETH", "price_yes": 78, "price_no": 22}]}
        r = await ls._try_first_take(None, 1, c_full, {}, sym,
                                     {**pos1, "entry_cents": 80})
        check("выше уровня, но ниже входа → не закрываем",
              r is False and len(settle_calls) == 1)
    finally:
        ls._settle_position = real_settle
        if real_get_em is not None:
            pt_stub.get_event_markets = real_get_em
        else:
            try:
                delattr(pt_stub, "get_event_markets")
            except AttributeError:
                pass

    keys = [k for k, _, _ in liq_menu.PARAMS]
    check("liq_tp_first_cents есть в меню", "liq_tp_first_cents" in keys)
    check("liq_tp_first_sec есть в меню", "liq_tp_first_sec" in keys)
    ok, norm, err = liq_menu.validate_manual_input("liq_tp_first_cents", "75")
    check("ручной ввод уровня 75¢", ok and norm == "75", err)
    ok, norm, err = liq_menu.validate_manual_input("liq_tp_first_sec", "999")
    check("окно 999с > максимума 240 → ошибка", not ok)

    print(f"\nИТОГ: {PASS} прошло, {FAIL} упало")
    return FAIL


if __name__ == "__main__":
    fails = asyncio.run(main())
    os.unlink(_TMP_DB.name)
    sys.exit(1 if fails else 0)
