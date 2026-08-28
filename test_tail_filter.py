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
  8. Вердикт по позиции — только после ОФИЦИАЛЬНОГО разрешения рынка:
     котировка 97-99¢ до разрешения больше не считается итогом
     (раздел 11, кейс SOL 20:30 27.08.2026).
  9. Цены выходов (аварийный выход, тейки) берутся из живого стакана
     CLOB, а не из отстающей гаммы (раздел 12, кейс XRP 21:10).
 10. Тонкий стакан: повторные попытки входа; при неудаче серия
     мартингейла НЕ сбрасывается — следующий сигнал заходит тем же
     лотом (раздел 13).
 11. Фильтр ликвидаций убыточной свечи: объём ликвидаций убыточного
     окна сравнивается с «сигнальным» окном (больше/меньше N%)
     (раздел 14).
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
                         carried_outcome=None, entry_note="", ref_liq_usd=None,
                         **_kw):
        entered.append({"series": series_, "stake": stake, "note": entry_note,
                        "ref_liq_usd": ref_liq_usd})

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

    print("\n=== 11. Вердикт только после официального разрешения рынка ===")
    # Кейс SOL 20:30 27.08.2026: рынок торговал наш исход по 99¢, но через
    # 52 секунды официально разрешился ПРОТИВ нас. Старая логика принимала
    # 99¢ за итог и записывала фантомную победу.
    pt_stub = sys.modules["polymarket_trading"]
    real_get_em = getattr(pt_stub, "get_event_markets", None)
    real_get_lp = getattr(pt_stub, "get_live_price", None)
    real_settle = ls._settle_position
    real_gwc = getattr(ls, "get_window_candle", None)
    settle_calls = []

    async def fake_settle(context, cid, c, state, symbol, pos, **kw):
        settle_calls.append({"symbol": symbol, **kw})

    async def no_candle(*a, **kw):
        return None

    ls._settle_position = fake_settle
    ls.get_window_candle = no_candle
    try:
        now_r = time.time()
        c_r = dict(ls.DEFAULTS)
        c_r["liq_timeframe"] = "5m"
        base_pos = {"slug": "sol-updown-5m-test", "outcome": "UP",
                    "window_start": now_r - 305, "window_end": now_r - 5,
                    "entry_cents": 53, "shares": 1.88, "stake": 1.0,
                    "series": 0, "is_demo": 1, "token_id": "tok_up"}
        st = {"positions": {"SOL": dict(base_pos)}}

        # 1. Рынок НЕ разрешён официально, но котировка 99¢ → ждём, вердикт
        #    не выносим (это и была фантомная победа).
        pt_stub.get_event_markets = lambda slug: {"markets": [{
            "question": "Sol", "price_yes": 99, "price_no": 1,
            "resolved": False}]}
        await ls._resolve_after_window(None, 1, None, c_r, st, "SOL",
                                       st["positions"]["SOL"])
        check("99¢ без официального разрешения → ждём, НЕ победа",
              len(settle_calls) == 0
              and st["positions"]["SOL"].get("awaiting_resolution") == 1,
              str(settle_calls))

        # 2. Рынок официально разрешился против нас → честный LOSS по 0¢.
        pt_stub.get_event_markets = lambda slug: {"markets": [{
            "question": "Sol", "price_yes": 0, "price_no": 100,
            "resolved": True}]}
        await ls._resolve_after_window(None, 1, None, c_r, st, "SOL",
                                       st["positions"]["SOL"])
        check("официальное разрешение против нас → LOSS по 0¢",
              len(settle_calls) == 1 and settle_calls[0]["win"] is False
              and settle_calls[0]["close_price"] == 0, str(settle_calls))
        check("reason указывает официальное разрешение рынка",
              "официальное разрешение" in str(
                  settle_calls[0].get("reason", "")))

        # 3. Официальное разрешение в нашу пользу → WIN по 100¢.
        st2 = {"positions": {"SOL": dict(base_pos)}}
        pt_stub.get_event_markets = lambda slug: {"markets": [{
            "question": "Sol", "price_yes": 100, "price_no": 0,
            "resolved": True}]}
        await ls._resolve_after_window(None, 1, None, c_r, st2, "SOL",
                                       st2["positions"]["SOL"])
        check("официальное разрешение в нашу пользу → WIN по 100¢",
              len(settle_calls) == 2 and settle_calls[1]["win"] is True
              and settle_calls[1]["close_price"] == 100, str(settle_calls))

        print("\n=== 12. Выходы по живому стакану, а не по отставшей гамме ===")
        # Кейс XRP 21:10 27.08.2026: гамма показывала 52¢, хотя рынок уже
        # шёл против нас; демо фиксировал «спасение в плюс» по 52¢. Теперь
        # цена берётся из живого стакана (best bid).
        settle_calls.clear()
        pt_stub.get_event_markets = lambda slug: {"markets": [{
            "question": "Xrp", "price_yes": 52, "price_no": 48}]}
        pt_stub.get_live_price = lambda tid: {"bid": 15, "ask": 18, "mid": 17}
        pos_x = {"slug": "xrp-updown-5m-test", "outcome": "UP",
                 "window_start": now_r - 298, "window_end": now_r + 2,
                 "entry_cents": 51, "shares": 1.96, "stake": 1.0,
                 "series": 1, "is_demo": 1, "token_id": "tok_xrp"}
        st_x = {"positions": {"XRP": dict(pos_x)}}
        await ls._check_open_position(None, 1, None, c_r, st_x, "XRP",
                                      st_x["positions"]["XRP"])
        check("аварийный выход: спасение по живому биду 15¢, а не гамме 52¢",
              len(settle_calls) == 1 and settle_calls[0]["win"] is False
              and settle_calls[0]["close_price"] == 15, str(settle_calls))
        check("reason отмечает источник цены (живой стакан)",
              "живой стакан" in str(settle_calls[0].get("reason", "")),
              str(settle_calls[0].get("reason", "")))

        # Стакан недоступен → откатываемся к цене гаммы (старое поведение).
        settle_calls.clear()
        pt_stub.get_live_price = lambda tid: None
        st_x2 = {"positions": {"XRP": dict(pos_x)}}
        await ls._check_open_position(None, 1, None, c_r, st_x2, "XRP",
                                      st_x2["positions"]["XRP"])
        check("нет стакана → спасение по гамме 52¢ (фолбэк)",
              len(settle_calls) == 1 and settle_calls[0]["win"] is False
              and settle_calls[0]["close_price"] == 52, str(settle_calls))

        # FAK-тейк первой ставки тоже берёт живой бид вместо гаммы.
        settle_calls.clear()
        pt_stub.get_event_markets = lambda slug: {"markets": [{
            "question": "Test ETH", "price_yes": 78, "price_no": 22}]}
        pt_stub.get_live_price = lambda tid: {"bid": 79, "ask": 80, "mid": 79}
        pos_ft = {"series": 0, "window_start": now_r - 5,
                  "window_end": now_r + 295, "outcome": "UP",
                  "entry_cents": 51, "shares": 10.0, "is_demo": 1,
                  "slug": "eth-updown-5m-test", "token_id": "tok_eth"}
        r = await ls._try_first_take(None, 1, c_full, {}, sym, dict(pos_ft))
        check("FAK-тейк: демо закрывает по живому биду 79¢ (гамма 78¢)",
              r is True and len(settle_calls) == 1
              and settle_calls[0]["close_price"] == 79, str(settle_calls))

        # Живой бид НИЖЕ уровня тейка → не закрываем, даже если гамма выше.
        settle_calls.clear()
        pt_stub.get_live_price = lambda tid: {"bid": 60, "ask": 62, "mid": 61}
        r = await ls._try_first_take(None, 1, c_full, {}, sym, dict(pos_ft))
        check("гамма 78¢, но живой бид 60¢ < 75¢ → тейк НЕ исполняем",
              r is False and len(settle_calls) == 0, str(settle_calls))
    finally:
        ls._settle_position = real_settle
        ls.get_window_candle = real_gwc
        for name, real in (("get_event_markets", real_get_em),
                           ("get_live_price", real_get_lp)):
            if real is not None:
                setattr(pt_stub, name, real)
            else:
                try:
                    delattr(pt_stub, name)
                except AttributeError:
                    pass

    print("\n=== 13. Тонкий стакан: повторные попытки и сохранение серии ===")
    check("дефолт попыток входа — 3", ls.DEFAULTS["liq_exec_retries"] == "3")
    check("дефолт паузы между попытками — 5с",
          ls.DEFAULTS["liq_exec_retry_sec"] == "5")

    set_cfg(liq_exec_retries="3", liq_exec_retry_sec="1",
            liq_spread_max_cents="3", liq_depth_mult="2")

    def thin_book(tid):
        return {"best_bid": 0.44, "best_ask": 0.52,
                "bids": [(0.44, 10)], "asks": [(0.52, 10)]}

    def ok_book(tid):
        return {"best_bid": 0.48, "best_ask": 0.50,
                "bids": [(0.48, 10)], "asks": [(0.50, 10)]}

    pt_stub.get_book = thin_book
    ok, reasons, _detail, attempts = await ls.check_execution_retrying(
        "tok", 1.0)
    check("стакан всё время тонкий → все 3 попытки, вход отменён",
          ok is False and attempts == 3 and len(reasons) > 0,
          f"ok={ok} attempts={attempts} reasons={reasons}")

    books_seq = [thin_book(None), thin_book(None), ok_book(None)]
    pt_stub.get_book = lambda tid: books_seq.pop(0)
    ok, _r, _d, attempts = await ls.check_execution_retrying("tok", 1.0)
    check("стакан восстановился → прошли с 3-й попытки",
          ok is True and attempts == 3, f"ok={ok} attempts={attempts}")

    pt_stub.get_book = thin_book
    ok, _r, _d, attempts = await ls.check_execution_retrying(
        "tok", 1.0, deadline=time.time() + 2)
    check("окно входа закрывается → попытки прекращены досрочно",
          ok is False and attempts == 1, f"ok={ok} attempts={attempts}")

    # Интеграция: шаг мартингейла не смог войти из-за стакана → серия
    # НЕ сбрасывается, следующий сигнал зайдёт тем же лотом.
    pt_stub.get_event_markets = lambda slug: {"markets": [{
        "question": "Btc", "token_yes": "tok_up", "token_no": "tok_dn",
        "price_yes": 51, "price_no": 49}]}
    pt_stub.get_market_info = lambda tid: {
        "neg_risk": False, "accepting_orders": True, "closed": False,
        "min_shares": 5.0, "min_size": 5.0, "tick_size": 0.01}
    set_cfg(liq_exec_retries="2", liq_exec_retry_sec="1", demo_mode="0")
    st_seed = ls._load_state()
    st_seed.setdefault("series", {})[sym] = 2
    st_seed.setdefault("series_pnl", {})[sym] = -2.0
    st_seed["positions"] = {}
    st_seed.setdefault("mg_pending", {}).pop(sym, None)
    ls._save_state(st_seed)

    real_send2 = ls._send
    captured_msg = {}

    async def fake_send2(context, cid, text, **kw):
        captured_msg["msg"] = text

    ls._send = fake_send2
    try:
        await ls._enter_martingale_step(None, 1, c_full, ls._load_state(),
                                        sym, 2.0, 2, "5m",
                                        carried_outcome="UP")
    finally:
        ls._send = real_send2
    st_after = ls._load_state()
    check("тонкий стакан → серия НЕ сброшена (шаг 2)",
          int((st_after.get("series") or {}).get(sym, 0)) == 2,
          str(st_after.get("series")))
    check("тонкий стакан → долг серии сохранён (-2.0$)",
          abs(float((st_after.get("series_pnl") or {}).get(sym, 0))
              + 2.0) < 1e-9,
          str(st_after.get("series_pnl")))
    check("сообщение: серия не остановлена, вход тем же лотом",
          "Серия НЕ остановлена" in captured_msg.get("msg", ""),
          captured_msg.get("msg", "")[:220])
    check("позиция не открыта",
          sym not in (st_after.get("positions") or {}))

    # Возврат демо-режима и очистка стакана-заглушки.
    set_cfg(demo_mode="1")
    try:
        delattr(pt_stub, "get_book")
    except AttributeError:
        pass
    st_clean = ls._load_state()
    st_clean.setdefault("series", {})[sym] = 0
    st_clean.setdefault("series_pnl", {}).pop(sym, None)
    st_clean.setdefault("cooldowns", {}).pop(sym, None)
    ls._save_state(st_clean)

    print("\n=== 14. Фильтр ликвидаций убыточной свечи ===")
    check("дефолты фильтра: выкл / 50% / больше",
          ls.DEFAULTS["liq_loss_liq_on"] == "0"
          and ls.DEFAULTS["liq_loss_liq_pct"] == "50"
          and ls.DEFAULTS["liq_loss_liq_mode"] == "above")

    # Подготовка: убыточное окно [ws, we) с ликвидациями 260k.
    pos14 = fresh_pos(sym)
    ws, we = pos14["window_start"], pos14["window_end"]
    loss_events = [{"time": ws + 10, "usd_value": 160_000},
                   {"time": ws + 20, "usd_value": 100_000}]
    ls._events_buffer[sym] = loss_events + [
        {"time": ws - 100, "usd_value": 999_999},   # ДО окна — не в счёт
        {"time": we + 100, "usd_value": 999_999},   # ПОСЛЕ окна — не в счёт
    ]
    check("события вне окна не учитываются",
          abs(ls.window_liq_usd(sym, ws, we) - 260_000) < 1e-9,
          str(ls.window_liq_usd(sym, ws, we)))

    r = ls.eval_loss_liq_filter(sym, 500_000, ws, we)
    check("фильтр выключен → не блокирует", r["blocked"] is False)

    set_cfg(liq_loss_liq_on="1", liq_loss_liq_pct="50",
            liq_loss_liq_mode="above", liq_tail_sec="0")

    r = ls.eval_loss_liq_filter(sym, 500_000, ws, we)
    check("режим «больше 50%»: 260k = 52% > 50% → входа НЕТ",
          r["blocked"] is True and abs(r["share_pct"] - 52.0) < 0.1,
          str(r))
    ls._events_buffer[sym] = [{"time": ws + 10, "usd_value": 240_000}]
    r = ls.eval_loss_liq_filter(sym, 500_000, ws, we)
    check("режим «больше 50%»: 240k = 48% < 50% → вход ЕСТЬ",
          r["blocked"] is False, str(r))
    ls._events_buffer[sym] = [{"time": ws + 10, "usd_value": 250_000}]
    r = ls.eval_loss_liq_filter(sym, 500_000, ws, we)
    check("ровно 50% → не блок (строгое неравенство)",
          r["blocked"] is False, str(r))

    set_cfg(liq_loss_liq_mode="below")
    ls._events_buffer[sym] = [{"time": ws + 10, "usd_value": 260_000}]
    r = ls.eval_loss_liq_filter(sym, 500_000, ws, we)
    check("режим «меньше 50%»: 260k > 50% → вход ЕСТЬ",
          r["blocked"] is False, str(r))
    ls._events_buffer[sym] = [{"time": ws + 10, "usd_value": 240_000}]
    r = ls.eval_loss_liq_filter(sym, 500_000, ws, we)
    check("режим «меньше 50%»: 240k < 50% → входа НЕТ",
          r["blocked"] is True, str(r))

    r = ls.eval_loss_liq_filter(sym, 0, ws, we)
    check("нет объёма сигнального окна → фильтр пропускается",
          r["blocked"] is False and "нет объёма" in r["why"], str(r))

    # Интеграция через расчёт убыточного шага (хвост выключен, чтобы он
    # не мешал): блок фильтра убыточной свечи = план ожидания окна.
    async def settle_loss_pos(pos):
        state = ls._load_state()
        state["positions"] = {sym: pos}
        state["series"] = {sym: 0}
        state.setdefault("series_pnl", {}).pop(sym, None)
        state.setdefault("mg_pending", {}).pop(sym, None)
        ls._save_state(state)
        await ls._settle_position(ctx, 1, ls.cfg(), state, sym, pos,
                                  win=False, close_price=0, price_yes=5,
                                  price_no=95,
                                  market_question="Bitcoin Up or Down",
                                  early_exit=True, settle_ts=time.time())

    set_cfg(liq_loss_liq_mode="above", liq_mg_skip_windows="2",
            liq_mg_skip_lot_pct="50")
    real_enter14 = ls._enter_martingale_step
    ls._enter_martingale_step = fake_enter
    try:
        entered.clear()
        pos_block = fresh_pos(sym)
        pos_block["ref_liq_usd"] = 500_000
        ls._events_buffer[sym] = [{"time": ws + 10, "usd_value": 260_000}]
        await settle_loss_pos(pos_block)
        st = ls._load_state()
        mg = (st.get("mg_pending") or {}).get(sym) or {}
        check("интеграция: 52% > 50% → входа нет, план ожидания (1/2)",
              len(entered) == 0 and mg.get("skips_used") == 1
              and int((st.get("series") or {}).get(sym, 0)) == 1,
              f"entered={entered} mg={mg}")

        entered.clear()
        pos_pass = fresh_pos(sym)
        pos_pass["ref_liq_usd"] = 500_000
        ls._events_buffer[sym] = [{"time": ws + 10, "usd_value": 240_000}]
        await settle_loss_pos(pos_pass)
        st = ls._load_state()
        check("интеграция: 48% < 50% → сразу ре-вход",
              len(entered) == 1 and entered[0]["series"] == 1, str(entered))
        check("ре-вход запоминает убыточное окно как «сигнальное» (240k)",
              entered and abs(float(entered[0].get("ref_liq_usd") or 0)
                              - 240_000) < 1e-9,
              str(entered))
        entered.clear()

        # Фильтр выключен — 52% не мешает входу.
        set_cfg(liq_loss_liq_on="0")
        ls._events_buffer[sym] = [{"time": ws + 10, "usd_value": 260_000}]
        pos_off = fresh_pos(sym)
        pos_off["ref_liq_usd"] = 500_000
        await settle_loss_pos(pos_off)
        check("фильтр выключен → 52% не блокирует ре-вход",
              len(entered) == 1, str(entered))
        entered.clear()
    finally:
        ls._enter_martingale_step = real_enter14
    set_cfg(liq_tail_sec="50", liq_loss_liq_on="0")

    keys = [k for k, _, _ in liq_menu.PARAMS]
    check("параметры фильтра убыточной свечи в меню",
          all(k in keys for k in ("liq_loss_liq_on", "liq_loss_liq_pct",
                                  "liq_loss_liq_mode")), str(keys))
    check("параметры попыток стакана в меню",
          all(k in keys for k in ("liq_exec_retries", "liq_exec_retry_sec")),
          str(keys))
    ok, norm, err = liq_menu.validate_manual_input("liq_loss_liq_mode", "больше")
    check("ручной ввод «больше» → above", ok and norm == "above", err)
    ok, norm, err = liq_menu.validate_manual_input("liq_loss_liq_mode", "меньше")
    check("ручной ввод «меньше» → below", ok and norm == "below", err)
    ok, norm, err = liq_menu.validate_manual_input("liq_loss_liq_on", "вкл")
    check("ручной ввод «вкл» → 1", ok and norm == "1", err)
    ok, norm, err = liq_menu.validate_manual_input("liq_exec_retries", "3")
    check("ручной ввод попыток 3", ok and norm == "3", err)
    ok, norm, err = liq_menu.validate_manual_input("liq_exec_retries", "99")
    check("попытки 99 > максимума 10 → ошибка", not ok)

    fl = ls._filters_status_line()
    check("фильтр убыточной свечи виден в статусе", "убыт.свеча" in fl, fl)
    el = ls._execution_status_line()
    check("попытки стакана видны в статусе",
          "попыток" in el and "сохраняется" in el, el)

    print(f"\nИТОГ: {PASS} прошло, {FAIL} упало")
    return FAIL


if __name__ == "__main__":
    fails = asyncio.run(main())
    os.unlink(_TMP_DB.name)
    sys.exit(1 if fails else 0)
