"""
Стратегия «Движение за рынком» (follow-the-candle) для Polymarket Up/Down.

ВТОРАЯ независимая торговая система. У неё свои настройки (ключи `td_*`),
своё состояние (`td_state`), свои фоновые задачи и СВОЯ статистика:
сделки помечаются в trade_history как strategy='trend' и нигде не
смешиваются со сделками стратегии «Каскад ликвидаций».

ЛОГИКА (v2 — вход «как у ликвидаций», ЗА N секунд до конца окна):
  1. За `td_entry_lead_sec` секунд до ЗАКРЫТИЯ текущего окна (рынка) бот
     смотрит, куда закрывается его свеча (живая свеча окна: close против
     open — тот же критерий, что у оракула Polymarket):
        свеча в плюс  → в НОВОМ окне (которое вот-вот начнётся) покупаем UP
        свеча в минус → в новом окне покупаем DOWN
     Ордер уходит ДО старта следующего рынка — бот успевает в цену ~50¢,
     пока стакан не уехал. Если момент упущен (нет данных/сбой), работает
     фолбэк-путь: вход в первые `td_entry_window_sec` секунд нового окна
     по последней ЗАКРЫТОЙ свече.
  2. Если в этот момент у нас открыта позиция по монете:
        свеча в НАШУ сторону («в плюс»)  → не трогаем: держим до профита
                                            (тейка) или до расчёта окна;
        свеча ПРОТИВ нас («в минус»)     → этот рынок закрываем сейчас
                                            (продажа по стакану), серия
                                            мартингейла +1, и СРАЗУ
                                            открываем следующий рынок в
                                            направлении этой свечи с
                                            увеличенным лотом.
  3. Вход — рыночным FAK-ордером (как и в стратегии ликвидаций):
     `place_market_order(..., order_type="FAK")`, сумма в USDC.
  4. Take-profit: цена нашего исхода дошла до `td_tp_cents` —
     продаём. Если размер позволяет поставить лимитку-«отложник»
     (>= минимума рынка) — выставляем GTC-лимит ровно на TP и ждём
     исполнения; иначе (мелкая позиция) — сразу продаём по стакану
     FAK-ордером.
  5. Профит → серия закрыта (счётчик мартингейла обнуляется).
     Убыток → серия продолжается: направление следующего входа диктует
     свеча, на которой мы проиграли (за неё и идём), лот — base × mult^шаг.
     Правило направления единое и для первого входа, и для отыгрыша.
  6. После конца окна невышедшая позиция рассчитывается: официально
     разрешённый рынок → закрытая свеча окна → (после grace) оценка по цене.

Изоляция от первой системы:
  • свои пары (td_symbols), свой вкл/выкл (td_active), свои интервалы;
  • позиции/серии в своём state — стратегии могут торговать одну монету
    одновременно и не мешать друг другу (полностью независимые lock'и);
  • статистика в trade_history разделена колонкой strategy.

Замечания по учёту:
  • TP-«отложник» в v1 трактуется как полный исполненный (все доли по
    цене TP); частичные исполнения на стакане Polymarket для мелких
    ставок — редкость, но учтены не полностью (зафиксировано в логах).
  • Итог окна определяется: сначала официальным разрешением рынка,
    затем закрытой свечой того же источника, что и сигнал, затем (после
    `td_settle_grace_sec` ожидания) грубо по цене нашего исхода.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone

import aiohttp

import liq_strategy as ls
import chainlink_price as clp
from database import (
    get_setting, set_setting, add_trade_history, get_trade_statistics,
    clear_trade_statistics,
)

log = logging.getLogger("bot.trend_strategy")

# Пометка стратегии в trade_history — по ней статистика разделяется.
STRATEGY_TAG = "trend"

STRATEGY_VERSION = "01.09.2026 #1 — следование за закрытой свечой, " \
                   "FAK-вход, TP отложником/FAK, мартингейл ×N, своя статистика"

# ===================== НАСТРОЙКИ =====================
DEFAULTS = {
    # Пары и таймфрейм — независимый от первой стратегии выбор.
    "td_symbols": '["BTC_USDT"]',
    "td_timeframe": "5m",
    # Источник свечи направления. chainlink — тот же TWAP-поток, по
    # которому Polymarket рассчитывает рынки; gate_spot — запасной.
    "td_candle_source": "chainlink",
    # Вход «как у ликвидаций»: за N секунд до КОНЦА текущего окна
    # оцениваем его свечу и ставим в следующее (ещё не начавшееся) окно —
    # так бот успевает войти, пока цена не ушла от 50¢.
    "td_entry_lead_sec": "2",
    # Фолбэк-путь: если границу упущен (нет свечи/сбой) — вход в первые
    # [delay..window] секунд УЖЕ начавшегося окна по последней закрытой свече.
    "td_entry_delay_sec": "0",
    "td_entry_window_sec": "60",
    # Максимальная цена входа (защита от входа в улетевшее окно). 0 — выкл.
    "td_entry_cap_cents": "70",
    # ===== ФИЛЬТР ФЛЕТА: сигнал против окна ПРЕДЫДУЩЕЙ свечи =====
    # На «пилообразном» рынке (свечи вверх-вниз-вверх-вниз) направление
    # последней свечи — шум, и вход по нему монетка. Фильтр сравнивает
    # «топливо» сигнального окна с предыдущим: каскад ликвидаций ($),
    # CVD-поток ($) и изменение OI (%). Порог — в % от предыдущего окна;
    # 0 — метрика выключена. У КАЖДОЙ метрики свой режим (они комбинируются
    # свободно): below — не входим, ЕСЛИ МЕНЬШЕ N% (движение не
    # подтвердилось = флет); above — если БОЛЬШЕ N% (шторм/перегрев).
    "td_liq_prev_pct": "0",
    "td_liq_prev_mode": "below",
    "td_cvd_prev_pct": "0",
    "td_cvd_prev_mode": "below",
    "td_oi_prev_pct": "0",
    "td_oi_prev_mode": "below",
    # Лот и мартингейл.
    "td_base_stake": "5",
    "td_martingale_mult": "2",
    "td_max_series": "5",
    # Take-profit.
    "td_tp_cents": "80",
    # auto — отложник (GTC-лимит на TP), если позволяет минимум рынка,
    # иначе FAK по стакану; limit — всегда лимитка; fak — всегда по рынку.
    "td_tp_mode": "auto",
    # Закрывать ли текущий рынок, если у его конца свеча идёт против нас
    # (и сразу открыть следующее окно в направлении этой свечи).
    "td_salvage_on": "1",
    # Сколько секунд после конца окна ждём официального расчёта рынка,
    # прежде чем верять закрытой свече/цене.
    "td_settle_grace_sec": "120",
    # Сколько позиций стратегия держит одновременно (по разным монетам).
    "td_max_concurrent": "1",
    # Интервалы фоновых задач (сек). Тик входа частый: нужно попасть
    # в полосу lead-секунд перед границей окна.
    "td_check_interval": "1",
    "td_scan_interval": "2",
    # Сколько последних сделок показывать в статистике.
    "td_recent_count": "10",
}

SIGNAL_STATS = "td_stat_entries"   # сколько входов реально исполнено
SKIP_STATS = "td_stat_skips"       # сколько входов отброшено
# Детализация пропусков по фильтру флета (сигнал vs предыдущая свеча):
FLAT_STAT_KEYS = {"liq": "td_stat_flat_liq", "cvd": "td_stat_flat_cvd",
                  "oi": "td_stat_flat_oi"}


def _stat(key: str) -> int:
    try:
        return int(float(get_setting(key, "0") or 0))
    except (TypeError, ValueError):
        return 0


def _bump_stat(key: str, n: int = 1):
    try:
        set_setting(key, str(_stat(key) + n))
    except Exception as e:
        log.debug(f"stat {key} err: {e}")


def reset_stats():
    set_setting(SIGNAL_STATS, "0")
    set_setting(SKIP_STATS, "0")


# ===================== ОБЩИЕ ХЕЛПЕРЫ =====================
async def _send(context, cid, text, **kwargs):
    """Отправка сообщения в чат с откатом на обычный текст (как в liq)."""
    kwargs.setdefault("parse_mode", "Markdown")
    body = text if len(text) <= 4000 else text[:4000]
    try:
        return await context.bot.send_message(cid, body, **kwargs)
    except Exception as e:
        if "parse entities" not in str(e).lower():
            log.warning(f"trend send err: {e}")
            return None
        log.warning(f"trend: Markdown битый, шлю текстом: {e}")
        kwargs.pop("parse_mode", None)
        try:
            return await context.bot.send_message(cid, body, **kwargs)
        except Exception as e2:
            log.warning(f"trend send plain err: {e2}")
            return None


def cfg() -> dict:
    out = {}
    for k, v in DEFAULTS.items():
        out[k] = get_setting(k, v)
    return out


def get_param(key: str):
    return get_setting(key, DEFAULTS.get(key, ""))


def set_param(key: str, value):
    set_setting(key, str(value))


def is_active() -> bool:
    return get_setting("td_active", "0") == "1"


def set_active(v: bool):
    set_setting("td_active", "1" if v else "0")


def _f_int(c: dict, key: str, default: int) -> int:
    try:
        return int(float(c.get(key, default)))
    except (TypeError, ValueError):
        return default


def _f_num(c: dict, key: str, default: float) -> float:
    try:
        return float(c.get(key, default))
    except (TypeError, ValueError):
        return default


def get_candle_source() -> str:
    v = str(get_setting("td_candle_source", DEFAULTS["td_candle_source"])
            or "").strip().lower()
    return v if v in ("chainlink", "gate_spot") else "chainlink"


# ===================== СОСТОЯНИЕ (ПЕРСИСТЕНТНОЕ) =====================
def _empty_state() -> dict:
    return {"positions": {}, "series": {}, "series_pnl": {},
            "last_window": {}, "try_cnt": {}, "bd_seen": {}, "oi_snap": {}}


def _load_state() -> dict:
    raw = get_setting("td_state", "")
    if not raw:
        return _empty_state()
    try:
        st = json.loads(raw)
        if not isinstance(st, dict):
            return _empty_state()
        for k in ("positions", "series", "series_pnl", "last_window",
                  "try_cnt", "bd_seen", "oi_snap"):
            if not isinstance(st.get(k), dict):
                st[k] = {}
        return st
    except Exception:
        return _empty_state()


def _save_state(state: dict):
    set_setting("td_state", json.dumps(state))


def reset_state():
    """Сброс серий/позиций. Осторожно: открытые позиции забываются."""
    _save_state(_empty_state())


def get_selected_symbols() -> list:
    raw = get_setting("td_symbols", DEFAULTS["td_symbols"]) or "[]"
    try:
        items = json.loads(raw)
        if not isinstance(items, list):
            return []
    except Exception:
        return []
    seen, out = set(), []
    for it in items:
        norm = ls._normalize_symbol(it) if isinstance(it, str) else None
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def set_selected_symbols(symbols: list):
    cleaned, seen = [], set()
    for it in symbols or []:
        norm = ls._normalize_symbol(it) if isinstance(it, str) else None
        if not norm or norm in seen:
            continue
        seen.add(norm)
        cleaned.append(norm)
    set_setting("td_symbols", json.dumps(cleaned))


# ===================== ЛОТ / МАРТИНГЕЙЛ =====================
POLY_MIN_STAKE_USD = 1.0  # минимум рыночного FAK-ордера по сумме, $


def compute_stake(c: dict, series: int) -> float:
    """Лот для шага серии: база × N^шаг (классический мартингейл)."""
    base = _f_num(c, "td_base_stake", 5.0)
    mult = _f_num(c, "td_martingale_mult", 2.0)
    series = max(0, int(series or 0))
    base = max(POLY_MIN_STAKE_USD, base)
    if series == 0:
        return round(base, 2)
    return round(base * (mult ** series), 2)


def get_series(state: dict, symbol: str) -> int:
    return int((state.get("series") or {}).get(symbol, 0) or 0)


def get_series_pnl(state: dict, symbol: str) -> float:
    try:
        return round(float((state.get("series_pnl") or {}).get(symbol, 0) or 0), 4)
    except (TypeError, ValueError):
        return 0.0


# ===================== СТАТИСТИКА (ТОЛЬКО СВОЯ) =====================
def get_trade_stats(is_demo: int) -> dict:
    """Сводка по сделкам ВТОРОЙ стратегии. Первая система сюда не попадает:
    фильтр — trade_history.strategy == 'trend'."""
    try:
        trades = get_trade_statistics(is_demo, strategy=STRATEGY_TAG)
    except Exception:
        trades = []
    if not trades:
        return {"total": 0, "wins": 0, "losses": 0, "winrate": 0,
                "total_pnl": 0, "avg_pnl": 0, "avg_win": 0, "avg_loss": 0,
                "profit_factor": 0, "best": 0, "worst": 0, "recent": [],
                "series_closed": 0}
    pnls = [float(t.get("pnl", 0) or 0) for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    total = len(pnls)
    total_pnl = sum(pnls)
    sum_win = sum(wins)
    sum_loss = abs(sum(losses)) if losses else 0
    pf = (sum_win / sum_loss) if sum_loss > 0 else (999 if sum_win > 0 else 0)
    try:
        recent_n = max(1, min(50, int(float(get_setting(
            "td_recent_count", DEFAULTS["td_recent_count"])))))
    except Exception:
        recent_n = 10
    return {
        "total": total, "wins": len(wins), "losses": len(losses),
        "winrate": round(len(wins) / total * 100, 1) if total else 0,
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(total_pnl / total, 2) if total else 0,
        "avg_win": round(sum_win / len(wins), 2) if wins else 0,
        "avg_loss": round(sum(losses) / len(losses), 2) if losses else 0,
        "profit_factor": round(pf, 2) if pf != 999 else 999,
        "best": round(max(pnls), 2), "worst": round(min(pnls), 2),
        "recent": sorted(trades, key=lambda x: x.get("timestamp", 0),
                         reverse=True)[:recent_n],
        "series_closed": len(wins),
    }


def clear_stats(is_demo: int):
    """Чистит статистику ТОЛЬКО второй стратегии (первую не трогаем)."""
    clear_trade_statistics(is_demo, strategy=STRATEGY_TAG)


def _stats_line(is_demo: int) -> str:
    st = get_trade_stats(is_demo)
    if not st["total"]:
        return "📊 Сделок пока нет (статистика второй стратегии пуста)"
    return (f"📊 *Статистика «за рынком»:* сделок {st['total']} | "
            f"WR *{st['winrate']}%* | PnL *{'+' if st['total_pnl'] >= 0 else ''}"
            f"{st['total_pnl']}$ | PF {st['profit_factor']}")


# ===================== СВЕЧИ ОКОН =====================
async def _candle_for_window(session, symbol: str, tf: str,
                             win_start: float, force: bool = False) -> dict | None:
    """Свеча конкретного окна Polymarket (для тренда — направление окна)."""
    if not win_start:
        return None
    dur = ls.TF_SECONDS.get(tf, 300)
    if get_candle_source() == "chainlink":
        try:
            cndl = clp.get_window_candle(symbol, win_start, win_start + dur, tf)
            if cndl:
                return cndl
        except Exception as e:
            log.debug(f"trend: chainlink candle err: {e}")
    candles = await ls.get_candles(session, symbol, tf, limit=5, force=force)
    target = int(win_start)
    for cndl in candles:
        if int(cndl.get("t", -1)) == target:
            return cndl
    return None


def _dir_emoji(direction: str) -> str:
    return "📈 UP" if direction == "UP" else "📉 DOWN"


# ===================== РАЗМЕРООПРЕДЕЛЕНИЕ И ИСПОЛНЕНИЕ =====================
def _sell_value_ok(pos: dict, shares: float, price_cents: int) -> bool:
    """Примет ли рынок лимитку-«отложник» на продажу: минимум в долях и $1."""
    min_shares = float(pos.get("min_shares") or ls.POLY_MIN_ORDER_SHARES)
    return (shares >= min_shares
            and shares * price_cents / 100.0 >= ls.POLY_MIN_NOTIONAL_USD)


def _sell_at(pos: dict, shares: float, price_cents: int) -> tuple[bool, str, object]:
    """Продажа долей: лимитка если можно, иначе рыночный FAK (как в liq)."""
    import polymarket_trading as pt

    price_cents = max(1, min(99, int(price_cents or 0)))
    price = price_cents / 100.0
    if shares * price < ls.POLY_MIN_NOTIONAL_USD:
        return False, "dust", None

    if _sell_value_ok(pos, shares, price_cents):
        try:
            res = pt.place_order(pos["token_id"], "SELL", price, shares,
                                 allow_min_bump=False)
            ok, _why = pt.is_order_accepted(res)
            if ok:
                return True, "limit", res
            log.warning(f"trend: sell-limit не прошёл: {res} ({_why})")
        except Exception as e:
            log.warning(f"trend: sell-limit exception: {e}")

    try:
        res = pt.place_market_order(pos["token_id"], "SELL", shares,
                                    order_type="FAK")
        ok, _why = pt.is_order_accepted(res)
        if ok:
            return True, "market", res
        log.warning(f"trend: sell-FAK не прошёл: {res} ({_why})")
        return False, "market", res
    except Exception as e:
        log.warning(f"trend: sell-FAK exception: {e}")
        return False, "market", None


def _sell_limit(pos: dict, shares: float, tp_cents: int):
    """Выставить GTC-«отложник» на продажу по TP. Возвращает order id | None."""
    import polymarket_trading as pt
    try:
        res = pt.place_order(pos["token_id"], "SELL", tp_cents / 100.0, shares)
        ok, why = pt.is_order_accepted(res)
        if not ok:
            log.warning(f"trend: TP-лимитка не прошла: {res} ({why})")
            return None
        oid = pt._extract_order_id(res)
        return oid or None
    except Exception as e:
        log.warning(f"trend: TP-лимитка exception: {e}")
        return None


def _tp_order_state(order_id: str) -> str:
    """Состояние выставленной TP-лимитки: 'open' | 'filled' | 'canceled' | 'unknown'."""
    import polymarket_trading as pt
    try:
        client = pt._client
        if client is not None and hasattr(client, "get_order"):
            raw = client.get_order(str(order_id))
            d = raw if isinstance(raw, dict) else pt._object_to_dict(raw)
            st = str((d or {}).get("status") or
                     (d or {}).get("order_status") or "").strip().lower()
            if st in ("live", "delayed", "new", "open"):
                return "open"
            if st in ("matched", "filled"):
                return "filled"
            if st in ("canceled", "cancelled"):
                return "canceled"
    except Exception as e:
        log.debug(f"trend: get_order({order_id}) err: {e}")
    # Фолбэк: есть ли ордер в списке открытых. Исчез дважды подряд → считаем
    # исполненным (cancel_all пользователь не делал, иначе бы сам знал).
    try:
        orders = pt.get_open_orders()
        if str(order_id) in {str(o.get("id")) for o in orders or []}:
            return "open"
        return "unknown"
    except Exception as e:
        log.debug(f"trend: get_open_orders err: {e}")
        return "unknown"


def _live_bid(token_id: str) -> int | None:
    """Сколько реально дадут за наши доли прямо сейчас (best bid), ¢."""
    import polymarket_trading as pt
    try:
        live = pt.get_live_price(token_id)
    except Exception as e:
        log.debug(f"trend: live price err: {e}")
        live = None
    if live and live.get("bid") is not None:
        try:
            return max(0, min(100, int(live["bid"])))
        except (TypeError, ValueError):
            return None
    return None


# ===================== ВХОД =====================
def _mark_window(symbol: str, win_start: int, burn: bool = False,
                 inc_try: int = 0) -> None:
    """Обновляет state (перечитывая свежий — без гонок): отмечает ЦЕЛЕВОЕ
    окно обработанным (burn) и/или поднимает счётчик попыток (inc_try = max
    попыток, после которых целевое окно выгорает)."""
    st = _load_state()
    changed = False
    if burn:
        st.setdefault("last_window", {})[symbol] = int(win_start)
        changed = True
    if inc_try > 0:
        t = st.setdefault("try_cnt", {})
        info = t.get(symbol) or {"w": 0, "n": 0}
        if int(info.get("w", 0)) != int(win_start):
            info = {"w": int(win_start), "n": 0}
        info["n"] = int(info.get("n", 0)) + 1
        if info["n"] >= inc_try:
            st.setdefault("last_window", {})[symbol] = int(win_start)
            t.pop(symbol, None)
        else:
            t[symbol] = info
        changed = True
    if changed:
        _save_state(st)


def _mark_bd(symbol: str, win_start: int):
    """Пометка «граница окна W уже оценена» — одно решение на границу."""
    st = _load_state()
    st.setdefault("bd_seen", {})[symbol] = int(win_start)
    _save_state(st)


async def _skip_and_note(symbol: str, reason: str):
    """Пишет причину в td_last_scan (видно в статусе) и в лог."""
    set_setting("td_last_scan", f"[{symbol}] {reason[:160]}")
    _bump_stat(SKIP_STATS)
    log.info(f"trend: {symbol} — {reason}")


def _live_ask(token_id: str) -> int | None:
    import polymarket_trading as pt
    try:
        live = pt.get_live_price(token_id)
    except Exception:
        live = None
    if live and live.get("ask") is not None:
        try:
            return max(0, min(100, int(live["ask"])))
        except (TypeError, ValueError):
            return None
    return None


# ===================== ФИЛЬТР ФЛЕТА (окно vs предыдущая свеча) =====================
FLAT_METRICS = ("liq", "cvd", "oi")


def _flat_cfg(c: dict) -> dict:
    """Настройки фильтра флета: {"liq": (порог%, режим), "cvd": ..., "oi": ...}.

    Режим у каждой метрики свой. Если конкретный режим не задан в БД,
    наследуется старый ОБЩИЙ td_prev_mode (совместимость с настройками,
    сохранёнными до разделения) — и лишь затем "below".
    """
    legacy = str(get_setting("td_prev_mode", "") or "").strip().lower()
    if legacy not in ("below", "above"):
        legacy = ""
    out = {}
    for key in FLAT_METRICS:
        pct = max(0.0, _f_num(c, f"td_{key}_prev_pct", 0.0))
        mode = str(get_setting(f"td_{key}_prev_mode", "") or "").strip().lower()
        if mode not in ("below", "above"):
            mode = legacy or "below"
        out[key] = (pct, mode)
    return out


def _flat_enabled(c: dict) -> bool:
    return any(pct > 0 for pct, _m in _flat_cfg(c).values())


async def _flat_ensure_streams():
    """WS-буферы ликвидаций и поток aggTrade общие на обе стратегии, а
    set_symbols() заменяет набор целиком — подписываем union монет обеих
    систем, иначе скан ликвидаций периодически выписывает наши монеты."""
    try:
        import liq_api as lqa
        import orderflow as ofl
        sf = ls.flow_symbols()
        if sf:
            lqa.set_symbols(sf)
            ofl.set_symbols(sf)
    except Exception as e:
        log.debug(f"trend: set_symbols union err: {e}")


def _cmp_ratio(cur: float | None, prev: float | None) -> float | None:
    """Доля текущего окна от предыдущего, %. None — данных нет."""
    if cur is None or prev is None:
        return None
    if prev <= 0:
        return 0.0 if cur <= 0 else float("inf")   # оба тише = флет (0%)
    return max(0.0, cur) / prev * 100.0


def _ratio_blocks(ratio: float | None, pct: float, mode: str) -> bool:
    if ratio is None or pct <= 0:
        return False
    return ratio < pct if mode == "below" else ratio > pct


def _ratio_note(name: str, ratio: float | None, pct: float, mode: str) -> str:
    arrow = "<" if mode == "below" else ">"
    if ratio is None:
        return f"{name}: нет данных (нужно ≥{pct:g}% {arrow})"
    rs = "∞" if ratio == float("inf") else f"{ratio:.0f}%"
    return f"{name}: {rs} от пред. окна (блок при {arrow}{pct:g}%)"


async def _flat_oi_record(symbol: str, win_start: int):
    """Снимок OI на границе окна: усреднённое изменение за последние ~5м
    с 4 бирж. Ключ — start окна, чьё изменение это описывает (для ТФ 5м
    совпадает точно; для других ТФ — приблизительно «последние 5 минут»)."""
    snaps = (_load_state().get("oi_snap") or {}).get(symbol) or {}
    if str(int(win_start)) in snaps:
        return  # уже записано на этой границе
    avg = None
    try:
        import liq_api as lqa
        import aiohttp as _ah
        async with _ah.ClientSession() as sess:
            oi = await lqa.get_multi_oi_change(sess, symbol)
        if oi and oi.get("average") is not None:
            avg = float(oi["average"])
    except Exception as e:
        log.debug(f"trend: oi snapshot err {symbol}: {e}")
    st = _load_state()
    snaps = st.setdefault("oi_snap", {}).setdefault(symbol, {})
    snaps[str(int(win_start))] = avg
    # храним последние 4 границы на монету
    if len(snaps) > 4:
        for k in sorted(snaps, key=lambda x: int(x))[:-4]:
            snaps.pop(k, None)
    _save_state(st)


async def _flat_check(session, c: dict, symbol: str, sig_start: int,
                      dur: int) -> tuple[str | None, str, float | None]:
    """Сравнение «топлива» сигнального окна с предыдущим (предыдущей свечой).

    Возвращает (ключ_счётчика|None, пояснение, ratio-для-лога). Блокирует,
    если какая-то включённая метрика попала в режим (below: меньше порога
    — флет; above: больше — перегрев). Недостаточно данных — НЕ блокирует."""
    fcfg = _flat_cfg(c)
    lp, lm = fcfg["liq"]
    cp, cm = fcfg["cvd"]
    op, om = fcfg["oi"]
    if lp <= 0 and cp <= 0 and op <= 0:
        return None, "", None
    sig_end = sig_start + dur
    prev_start = sig_start - dur
    notes, blocked_by, blocked_ratio = [], None, None

    # --- 1. Ликвидации: сумма $ в окнах ---
    if lp > 0:
        try:
            import liq_api as lqa
            alive = lqa.ws_liquid_ready()
            cur = prev = None
            if alive:
                evs = await lqa.recent_liquidations(symbol, min_usd=1000.0,
                                                     since=prev_start - 5)
                cur = sum(float(e.get("usd_value", 0) or 0) for e in evs
                          if sig_start <= float(e.get("time", 0) or 0) < sig_end)
                prev = sum(float(e.get("usd_value", 0) or 0) for e in evs
                           if prev_start <= float(e.get("time", 0) or 0) < sig_start)
            r = _cmp_ratio(cur, prev)
            notes.append(_ratio_note(f"лика ${cur:,.0f}/${prev:,.0f}"
                                     if cur is not None else "лика",
                                     r, lp, lm))
            if _ratio_blocks(r, lp, lm):
                blocked_by, blocked_ratio = "liq", r
        except Exception as e:
            log.debug(f"trend: flat liq err {symbol}: {e}")
            notes.append(f"лика: сбой ({e})")

    # --- 2. CVD: |знаковый поток $| в окнах ---
    if blocked_by is None and cp > 0:
        try:
            import orderflow as ofl
            st = ofl.status() or {}
            alive = bool(st.get("connected")) and (st.get("age_sec") or 9e9) < 120
            cur = prev = None
            if alive:
                f_cur = ofl.flow_stats(symbol, sig_start, sig_end)
                f_prev = ofl.flow_stats(symbol, prev_start, sig_start)
                if f_cur is not None or f_prev is not None:
                    cur = abs(float((f_cur or {}).get("cvd") or 0))
                    prev = abs(float((f_prev or {}).get("cvd") or 0))
            r = _cmp_ratio(cur, prev)
            notes.append(_ratio_note(f"cvd ${cur:,.0f}/${prev:,.0f}"
                                     if cur is not None else "cvd",
                                     r, cp, cm))
            if _ratio_blocks(r, cp, cm):
                blocked_by, blocked_ratio = "cvd", r
        except Exception as e:
            log.debug(f"trend: flat cvd err {symbol}: {e}")
            notes.append(f"cvd: сбой ({e})")

    # --- 3. OI: |изменение %| по снапшотам границ ---
    if blocked_by is None and op > 0:
        try:
            snaps = (_load_state().get("oi_snap") or {}).get(symbol) or {}
            cur = snaps.get(str(int(sig_start)))
            prev = snaps.get(str(int(sig_start - dur)))
            cur = abs(float(cur)) if cur is not None else None
            prev = abs(float(prev)) if prev is not None else None
            r = _cmp_ratio(cur, prev)
            notes.append(_ratio_note(f"oi {cur if cur is None else f'{cur:.2f}%'}"
                                     f"/{prev if prev is None else f'{prev:.2f}%'}"
                                     if cur is not None or prev is not None else "oi",
                                     r, op, om))
            if _ratio_blocks(r, op, om):
                blocked_by, blocked_ratio = "oi", r
        except Exception as e:
            log.debug(f"trend: flat oi err {symbol}: {e}")
            notes.append(f"oi: сбой ({e})")

    if blocked_by is None:
        return None, " | ".join(notes), None
    return FLAT_STAT_KEYS[blocked_by], " | ".join(notes), blocked_ratio


async def _open_in_window(context, cid, session, c: dict, symbol: str,
                          target_start: int, outcome: str, candle: dict | None,
                          src_note: str = "", sig_start: int = 0) -> bool:
    """Ставит вход в ЦЕЛЕВОЕ окно `target_start` в направлении `outcome`.

    Общий механизм для обоих путей: «у конца текущего окна — в следующее»
    (основной, цена ещё у 50¢) и фолбэк «в начало уже начавшегося окна».
    Возвращает True, если позиция открыта.
    """
    import polymarket_trading as pt

    tf = str(c.get("td_timeframe", "5m"))
    dur = ls.TF_SECONDS.get(tf, 300)
    state = _load_state()
    if symbol in (state.get("positions") or {}):
        return False
    if int((state.get("last_window") or {}).get(symbol, -1)) == int(target_start):
        return False  # в это окно уже входили/осознанно пропустили

    tcnt = (state.get("try_cnt") or {}).get(symbol) or {}
    if int(tcnt.get("w", 0)) == int(target_start) and int(tcnt.get("n", 0)) >= MAX_ATTEMPTS:
        return False

    max_conc = max(1, _f_int(c, "td_max_concurrent", 1))
    if len(state.get("positions") or {}) >= max_conc:
        return False

    # --- Фильтр флета: сигнальное окно против предыдущей свечи ---
    # Вход по направлению свечи имеет смысл, когда за свечей стоит живое
    # движение (ликвидации/поток/OI). Если метрики сигнального окна угасли
    # относительно предыдущего (или, наоборот, шторм) — пропускаем окно.
    if sig_start and _flat_enabled(c):
        f_key, f_note, _fr = await _flat_check(session, c, symbol,
                                               int(sig_start), dur)
        if f_key:
            _mark_window(symbol, target_start, burn=True)
            _bump_stat(f_key)
            await _skip_and_note(symbol, f"флет: {f_note}")
            await _send(context, cid,
                        f"🌫 *Вход пропущен — флет* `{symbol}` {outcome}\n"
                        f"📉 {ls.md_escape(f_note)}\n"
                        f"_Сигнальное окно не подтверждено движением "
                        f"относительно предыдущей свечи — ждём "
                        f"следующей границы._")
            return False

    # --- Рынок целевого окна (может быть ещё не запущен: вход «заранее») ---
    slug = ls.build_updown_slug(symbol, tf, target_start, 0)
    info = pt.get_event_markets(slug)
    if not info or not info.get("markets"):
        _mark_window(symbol, target_start, inc_try=5)
        await _skip_and_note(symbol, f"рынок {slug} не найден (retry до 5 попыток)")
        return False

    m = info["markets"][0]
    token_id = m["token_yes"] if outcome == "UP" else m["token_no"]
    gamma_cents = int(m.get("price_yes") if outcome == "UP" else m.get("price_no")) or 50
    # Живой аск точнее, чем гамма (она отстаёт): по нему и кэп, и оценка входа.
    ask = _live_ask(token_id)
    entry_cents = max(1, min(99, ask if ask else gamma_cents))

    # --- Кэп цены входа ---
    cap = _f_int(c, "td_entry_cap_cents", 0)
    if cap and entry_cents > cap:
        _mark_window(symbol, target_start, burn=True)
        await _skip_and_note(symbol, f"цена входа {entry_cents}¢ > кэпа {cap}¢")
        await _send(context, cid,
                     f"🚫 *Вход пропущен* `{symbol}` {outcome}\n"
                     f"🎯 Цена {entry_cents}¢ выше кэпа *{cap}¢* — догонять окно невыгодно.")
        return False

    # --- Лот с мартингейлом ---
    series = get_series(_load_state(), symbol)
    stake_usd = compute_stake(c, series)
    market_info = pt.get_market_info(token_id)
    plan = ls.plan_order(stake_usd, entry_cents, market_info, order_kind="market")
    if plan["below_min"]:
        # Мелочь, которую рынок не примет, — доводим до минимума FAK ($1)
        stake_usd = max(plan["min_cost"], POLY_MIN_STAKE_USD)
        plan = ls.plan_order(stake_usd, entry_cents, market_info, order_kind="market")
    shares = plan["shares"]

    demo = get_setting("demo_mode", "0") == "1"
    is_demo_flag = 1 if demo else 0

    # --- Баланс до ордера ---
    if not demo:
        try:
            bal = pt.get_balance()
        except Exception as e:
            log.warning(f"trend: get_balance err: {e}")
            bal = None
        if bal is not None and bal < stake_usd - 0.005:
            _mark_window(symbol, target_start, burn=True)
            await _skip_and_note(symbol, f"нужно ${stake_usd:.2f}, баланс ${bal:.2f}")
            await _send(context, cid,
                        f"⛔ *Недостаточно USDC* `{symbol}` {outcome}\n"
                        f"💵 Нужно: *${stake_usd:.2f}* | Доступно: *${bal:.2f}*")
            return False

    # --- FAK-ордер ---
    if not demo and not pt.is_ready():
        # Ключей Polymarket нет — гонять ордера бессмысленно.
        _mark_window(symbol, target_start, burn=True)
        await _skip_and_note(symbol, "Polymarket API не инициализирован (не в демо-режиме)")
        await _send(context, cid,
                    f"⚠️ `{symbol}` {outcome}: Polymarket API не подключён — "
                    f"войдите в «💰 Торговля → ⚙️ API Настройки» или включите демо-режим.")
        return False
    order_ok, fill = True, None
    if not demo:
        res = pt.place_market_order(token_id, "BUY", stake_usd, order_type="FAK")
        order_ok, why = pt.is_order_accepted(res)
        if not order_ok:
            # Стакан/сеть могут поправиться к следующему тiku — до MAX_ATTEMPTS.
            _mark_window(symbol, target_start, inc_try=MAX_ATTEMPTS)
            await _skip_and_note(symbol, f"FAK не принят: {why}")
            return False
        try:
            fill = pt._extract_fill(res, "BUY")
        except Exception:
            fill = None

    if fill and fill.get("shares"):
        shares = round(float(fill["shares"]), 4)
        cost = fill.get("cost")
        fprice = fill.get("price")
        stake_usd = round(float(cost) if cost else stake_usd, 4)
        if fprice:
            entry_cents = max(1, min(99, int(round(float(fprice) * 100))))
        elif shares > 0:
            entry_cents = max(1, min(99, int(round(stake_usd / shares * 100))))
    elif not demo and shares > 0:
        entry_cents = max(1, min(99, int(round(stake_usd / shares * 100))))

    # --- Пишем позицию (перечитаем state — защита от гонки) ---
    state = _load_state()
    state.setdefault("positions", {})
    if symbol in state["positions"]:
        log.warning(f"trend: гонка — у {symbol} уже есть позиция")
        return False
    now_ts = time.time()
    pos = {
        "symbol": symbol,
        "slug": slug,
        "token_id": token_id,
        "outcome": outcome,
        "stake": round(stake_usd, 4),
        "shares": shares,
        "entry_cents": entry_cents,
        "min_shares": ls.limit_min_shares(market_info),
        "series": series,
        "window_start": int(target_start),
        "window_end": int(target_start) + dur,
        "open_ts": now_ts,
        "is_demo": is_demo_flag,
        "market_question_raw": (m.get("question") or ""),
        "prev_candle": {"open": (candle or {}).get("open"),
                        "close": (candle or {}).get("close"),
                        "t": (candle or {}).get("t"),
                        "src": (candle or {}).get("src")},
    }
    state["positions"][symbol] = pos
    state.setdefault("last_window", {})[symbol] = int(target_start)
    state.setdefault("try_cnt", {}).pop(symbol, None)
    _save_state(state)
    _bump_stat(SIGNAL_STATS)

    delta_pct = 0.0
    try:
        if candle and candle.get("open"):
            delta_pct = (candle["close"] - candle["open"]) / candle["open"] * 100
    except Exception:
        pass
    start_str = datetime.fromtimestamp(target_start, tz=timezone.utc).strftime("%H:%M:%S UTC")
    mode_emoji = "🎮 ДЕМО" if is_demo_flag else "💰 РЕАЛ"
    tp_cents = _f_int(c, "td_tp_cents", 80)
    ser_note = (f" | ♻️ шаг серии *{series}/{_f_int(c, 'td_max_series', 5)}* "
                f"(лот ×{_f_num(c, 'td_martingale_mult', 2):g})" if series > 0 else "")
    await _send(
        context, cid,
        f"🕯 *ВХОД ЗА РЫНКОМ* `{symbol}` {_dir_emoji(outcome)} {mode_emoji}\n\n"
        f"📊 {src_note or 'Свеча'}: *{outcome}* ({delta_pct:+.3f}%)\n"
        f"📈 `{slug}` — старт окна *{start_str}*\n"
        f"💵 Лот: *${pos['stake']:.2f}* по {entry_cents}¢ "
        f"({shares:g} долей, FAK){ser_note}\n"
        f"🏁 TP: *{tp_cents}¢*",
    )
    log.info(f"trend: ✅ вход {symbol} {outcome} ${pos['stake']:.2f} "
             f"@ {entry_cents}¢ (серия {series}, окно {target_start})")
    return True


async def _boundary_close(context, cid, session, c: dict, state: dict,
                          symbol: str, pos: dict, candle_now: str):
    """Граница окна: свеча против нас — закрываем рынок по стакану (FAK)."""
    import polymarket_trading as pt

    shares = float(pos.get("shares") or 0)
    is_demo_flag = pos.get("is_demo", 0)
    bid = _live_bid(pos["token_id"])
    if bid is None:
        try:
            info = pt.get_event_markets(pos["slug"])
            if info and info.get("markets"):
                mm = info["markets"][0]
                o = pos.get("outcome")
                bid = int(mm.get("price_yes") if o == "UP" else mm.get("price_no")) or 0
        except Exception:
            bid = 0
    bid = max(0, int(bid or 0))
    delta_pct = 0.0
    try:
        wc = await _candle_for_window(session, symbol, str(c.get("td_timeframe", "5m")),
                                      pos.get("window_start") or 0, force=True)
        if wc and wc.get("open"):
            delta_pct = (wc["close"] - wc["open"]) / wc["open"] * 100
    except Exception:
        pass

    sell_price = bid if bid > 0 else 1
    ok_sell, mode, res = (True, "demo", None) if is_demo_flag else \
        _sell_at(pos, shares, sell_price)
    realized = 0.0
    if ok_sell:
        fill = None
        if not is_demo_flag:
            try:
                fill = pt._extract_fill(res, "SELL") if mode == "market" else None
            except Exception:
                fill = None
        if fill and fill.get("shares"):
            sold = float(fill["shares"])
            got = fill.get("cost")
            realized = float(got) if got else sold * sell_price / 100.0
        else:
            realized = shares * sell_price / 100.0
    close_px = int(round(realized / shares * 100)) if shares > 0 and realized > 0 else 0
    await _settle(context, cid, c, state, symbol, pos,
                  win=False,
                  close_price=close_px if ok_sell else 0,
                  reason=(f"свеча у конца окна {candle_now} против "
                          f"{_dir_emoji(pos.get('outcome'))} ({delta_pct:+.3f}%) — "
                          f"рынок закрыт ({'отложник' if mode == 'limit' else 'FAK'})"
                          if ok_sell else
                          f"свеча у конца окна {candle_now} против "
                          f"{_dir_emoji(pos.get('outcome'))}, остаток продать "
                          f"не удалось (пыль)"),
                  early_exit=True, realized=realized)


MAX_ATTEMPTS = 8  # попыток входа в одно целевое окно (FAK/сеть могут штормить)


# ===================== ВЕДЕНИЕ ПОЗИЦИИ =====================
async def _manage_position(context, cid, session, c: dict, state: dict,
                          symbol: str, pos: dict):
    import polymarket_trading as pt

    now = time.time()
    outcome = pos.get("outcome")
    token_id = pos["token_id"]
    tp_cents = max(2, min(99, _f_int(c, "td_tp_cents", 80)))
    time_left = float(pos.get("window_end", 0)) - now

    # --- 0. Исполнение TP-«отложника» ---
    if pos.get("tp_order_id"):
        st = _tp_order_state(pos["tp_order_id"])
        if st == "filled":
            pos.pop("tp_order_id", None)
            await _settle(context, cid, c, state, symbol, pos,
                          win=True, close_price=int(pos.get("tp_order_price", tp_cents)),
                          reason="TP-отложник исполнен", early_exit=True)
            return
        if st in ("canceled",):
            pos.pop("tp_order_id", None)
            state.setdefault("positions", {})[symbol] = pos
            _save_state(state)
            # продолжаем обычный цикл на следующем тике
        elif st == "unknown":
            pos["tp_unknown_cnt"] = int(pos.get("tp_unknown_cnt", 0)) + 1
            if pos["tp_unknown_cnt"] >= 3 and time_left > 2:
                # 3 проверки подряд ордер не виден в открытых — считаем исполненным
                pos.pop("tp_order_id", None)
                await _settle(context, cid, c, state, symbol, pos,
                              win=True, close_price=tp_cents,
                              reason="TP-отложник исчез из открытых (считаю исполненным)",
                              early_exit=True)
                return
            state.setdefault("positions", {})[symbol] = pos
            _save_state(state)
        # open / unknown(мало проверок): ждём; на последних секундах снимаем
        if pos.get("tp_order_id") and time_left <= 2:
            try:
                pt.cancel_order(pos["tp_order_id"])
            except Exception as e:
                log.debug(f"trend: cancel TP-order err: {e}")
            pos.pop("tp_order_id", None)
            state.setdefault("positions", {})[symbol] = pos
            _save_state(state)
        return

    # --- 1. Окно закрылось → расчёт ---
    if time_left <= 0:
        await _resolve_window(context, cid, session, c, state, symbol, pos)
        return

    # --- 2. Живая цена решения ---
    bid = _live_bid(token_id)
    if bid is None:
        try:
            info = pt.get_event_markets(pos["slug"])
            if info and info.get("markets"):
                mm = info["markets"][0]
                bid = int(mm.get("price_yes") if outcome == "UP" else mm.get("price_no")) or None
        except Exception as e:
            log.debug(f"trend: gamma price err: {e}")
    bid = int(bid) if bid is not None else 0

    # --- 3. Take-profit ---
    if bid >= tp_cents and not pos.get("awaiting_resolution"):
        await _try_take_profit(context, cid, c, state, symbol, pos, bid, tp_cents)
        return

    # Иначе — ждём: либо догоним TP, либо дотерпим до расчёта окна.
    return


async def _try_take_profit(context, cid, c: dict, state: dict, symbol: str,
                           pos: dict, bid: int, tp_cents: int):
    """TP достигнут: продаём отложником (если можно) или сразу FAK'ом."""
    import polymarket_trading as pt

    shares = float(pos.get("shares") or 0)
    entry_cents = int(pos.get("entry_cents") or 0)
    is_demo_flag = pos.get("is_demo", 0)
    mode = str(c.get("td_tp_mode", "auto")).strip().lower()
    if mode not in ("auto", "limit", "fak"):
        mode = "auto"

    if is_demo_flag:
        await _settle(context, cid, c, state, symbol, pos, win=True,
                      close_price=tp_cents,
                      reason=f"🎮 ДЕМО: TP {tp_cents}¢ достигнут (бид {bid}¢)",
                      early_exit=True)
        return

    can_limit = _sell_value_ok(pos, shares, tp_cents)
    use_limit = (mode == "limit") or (mode == "auto" and can_limit)
    if mode == "limit" and not can_limit:
        log.info(f"trend: {symbol} режим limit, но отложник не проходит по минимуму — FAK")
        use_limit = False

    if use_limit:
        oid = _sell_limit(pos, shares, tp_cents)
        if oid:
            pos["tp_order_id"] = str(oid)
            pos["tp_order_price"] = tp_cents
            pos["tp_placed_at"] = time.time()
            state.setdefault("positions", {})[symbol] = pos
            _save_state(state)
            await _send(context, cid,
                        f"🎯 *TP {tp_cents}¢ — отложник* `{symbol}` {pos['outcome']}\n"
                        f"SELL-лимит на {tp_cents}¢ выставлен ({shares:g} долей), ждём исполнения.\n"
                        f"💵 Вход был {entry_cents}¢.")
            return
        # не удалось поставить — падаем на FAK
    ok, mode_used, res = _sell_at(pos, shares, max(tp_cents, bid))
    if not ok:
        log.warning(f"trend: {symbol} TP-продажа не удалась — ждём следующего тика")
        return
    realized = None
    try:
        fill = pt._extract_fill(res, "SELL") if mode_used == "market" else None
    except Exception:
        fill = None
    if fill and fill.get("shares"):
        got = fill.get("cost")
        realized = float(got) if got else float(fill["shares"]) * max(tp_cents, bid) / 100.0
    if realized is None:
        realized = shares * tp_cents / 100.0
    close_px = int(round(realized / shares * 100)) if shares > 0 else tp_cents
    await _settle(context, cid, c, state, symbol, pos,
                  win=close_px > entry_cents, close_price=close_px,
                  reason=(f"TP: продано по рынку FAK ≈{close_px}¢"
                          if mode_used == "market"
                          else f"TP: лимитка на {tp_cents}¢ исполнена немедленно"),
                  early_exit=True, realized=realized)


# ===================== РАСЧЁТ ОКНА =====================
async def _resolve_window(context, cid, session, c: dict, state: dict,
                          symbol: str, pos: dict):
    """Окно истекло — определяем итог и рассчитываем позицию."""
    import polymarket_trading as pt

    # На всякий случай снимаем незакрытую TP-лимитку.
    if pos.get("tp_order_id"):
        try:
            pt.cancel_order(pos["tp_order_id"])
        except Exception:
            pass
        pos.pop("tp_order_id", None)

    now = time.time()
    tf = str(c.get("td_timeframe", "5m"))
    dur = ls.TF_SECONDS.get(tf, 300)
    outcome = pos["outcome"]
    opposite = "DOWN" if outcome == "UP" else "UP"
    window_start = pos.get("window_start") or (pos.get("window_end", 0) - dur)
    waited = now - float(pos.get("window_end", now))

    # 1. Официальное разрешение рынка — источник истины по выплатам.
    price_yes = price_no = 0
    resolved = False
    try:
        info = pt.get_event_markets(pos["slug"])
        if info and info.get("markets"):
            mm = info["markets"][0]
            price_yes = mm.get("price_yes") or 0
            price_no = mm.get("price_no") or 0
            resolved = bool(mm.get("resolved"))
    except Exception as e:
        log.debug(f"trend: resolve gamma err: {e}")
    our_price = price_yes if outcome == "UP" else price_no
    # До официального разрешения цена — живая котировка, ей не верим
    # (проигравший исход может стоить 97¢ за минуту до разворота в 0).
    # После разрешения победитель стоит ~100¢, проигравший ~0¢.
    market_decided = resolved and (our_price >= 97 or our_price <= 3)

    # 2. Закрытая свеча окна — второй авторитет (Chainlink TWAP = тот же
    #    оракул, что считает рынок; gate_spot — запасной и может разойтись).
    wc = await _candle_for_window(session, symbol, tf, window_start, force=True)
    candle_result = ls.resolve_state(wc) if (wc and wc.get("closed")) else None

    grace = max(0, min(600, _f_int(c, "td_settle_grace_sec", 120)))
    if market_decided:
        result = outcome if our_price >= 50 else opposite
        reason = f"расчёт рынка: {outcome}@{our_price}¢ (официально разрешён)"
    elif candle_result:
        result = candle_result
        src = str((wc or {}).get("src") or "?")
        reason = f"рынок ещё не разрешён — итог по закрытой свече ({src})"
        if "gateio" in src:
            reason += " ⚠️ сверь с Polymarket"
    elif waited < grace:
        pos["awaiting_resolution"] = 1
        state.setdefault("positions", {})[symbol] = pos
        _save_state(state)
        set_setting("td_last_scan",
                    f"[{symbol}] окно закрыто, жду расчёта ({int(waited)}с)")
        return
    else:
        # Ни разрешения, ни свечи — грубая оценка по живой цене, чтобы не
        # держать позицию вечно.
        bid = _live_bid(pos["token_id"])
        px = bid if bid is not None else our_price
        result = outcome if (px or 0) >= 50 else opposite
        reason = f"нет расчёта и свечи — оценка по цене {px}¢, сверь с Polymarket"

    win = (result == outcome)
    log.info(f"trend: 🏁 {symbol} окно рассчитано: {result} → {'WIN' if win else 'LOSS'}")
    await _settle(context, cid, c, state, symbol, pos, win=win,
                  close_price=100 if win else 0,
                  reason=reason, early_exit=False)


# ===================== РАСЧЁТ СДЕЛКИ / СЕРИИ =====================
async def _settle(context, cid, c: dict, state: dict, symbol: str, pos: dict, *,
                  win: bool, close_price: int, reason: str = "",
                  early_exit: bool = False, realized: float | None = None):
    """Фиксирует PnL сделки, ведёт серию мартингейла, шлёт отчёт.

    Профит → серия закрыта (сброс в 0). Убыток → серия шага +1: вход
    в следующее окно возьмёт лот base×N^шаг автоматически (entry scan).
    Если серия упёрлась в td_max_series — сброс («серия слита»).
    """
    state = _load_state()
    positions_map = state.setdefault("positions", {})
    series_map = state.setdefault("series", {})
    pnl_map = state.setdefault("series_pnl", {})

    # Защита от двойного расчёта одной позиции двумя проходами.
    cur = positions_map.get(symbol)
    if not cur or abs(float(cur.get("open_ts") or 0) - float(pos.get("open_ts") or 0)) > 1.0:
        log.info(f"trend: {symbol} позиция уже закрыта другим проходом")
        return

    stake = float(pos.get("stake") or 0)
    shares = float(pos.get("shares") or 0)
    entry_cents = int(pos.get("entry_cents") or 0)
    is_demo_flag = pos.get("is_demo", 0)
    slug = pos.get("slug", "")
    outcome = pos.get("outcome", "?")
    current_series = int(pos.get("series", 0) or 0)

    if realized is not None:
        pnl = round(float(realized) - stake, 2)
    elif shares > 0:
        pnl = round(shares * (close_price / 100.0) - stake, 2)
    elif entry_cents > 0:
        pnl = round(stake * (close_price - entry_cents) / entry_cents, 2)
    else:
        pnl = 0.0
    # Правило из ТЗ: серия закрывается ПОСЛЕ ПОЛУЧЕННОГО ПРОФИТА.
    # Поэтому победителем шаг считается только если итог сделки > 0
    # (например, спасённый остаток продали дороже входа — это профит,
    # а «тейк» по цене ниже входа — это убыток, серия продолжается).
    win = pnl > 0

    trade_question = ls._format_trade_question(
        symbol, pos.get("market_question_raw") or "")
    add_trade_history(is_demo_flag, slug, trade_question, outcome, "BUY",
                      shares if shares > 0 else stake,
                      entry_cents, int(close_price), pnl,
                      strategy=STRATEGY_TAG)

    prev_series_pnl = get_series_pnl(state, symbol)
    series_total = round(prev_series_pnl + pnl, 4)
    mode_emoji = "🎮 ДЕМО" if is_demo_flag else "💰 РЕАЛ"
    max_series = max(1, _f_int(c, "td_max_series", 5))
    tag = "⏰ ДОСРОЧНО" if early_exit else "🏁 В СРОК"
    stats_after = get_trade_stats(is_demo_flag)
    stats_str = (f"📊 По этой стратегии: {stats_after['total']} сделок, "
                 f"WR {stats_after['winrate']}%, "
                 f"PnL {'+' if stats_after['total_pnl'] >= 0 else ''}"
                 f"{stats_after['total_pnl']}$")

    if win:
        series_map[symbol] = 0
        pnl_map.pop(symbol, None)
        positions_map.pop(symbol, None)
        _save_state(state)
        nxt = compute_stake(c, 0)
        msg = (f"✅ *СДЕЛКА В ПЛЮС — серия закрыта* `{symbol}` {tag} {mode_emoji}\n"
               f"📈 `{slug}` {outcome}: вход {entry_cents}¢ → выход {int(close_price)}¢\n"
               f"💵 {stake:.2f}$ → {'+' if pnl >= 0 else ''}{pnl}$"
               + (f" | Итог серии: *{'+' if series_total >= 0 else ''}{series_total:.2f}$*"
                  if current_series > 0 else "")
               + f"\n♻️ Следующий вход — базовым лотом *${nxt:.2f}*\n"
               + (f"🧭 {ls.md_escape(reason)}\n" if reason else "")
               + stats_str)
        await _send(context, cid, msg[:4000])
        return

    # === Убыток: серия продолжается ===
    if series_total >= 0:
        # Убыточный по знаку шаг, но серию уже отыграли в плюс/ноль.
        series_map[symbol] = 0
        pnl_map.pop(symbol, None)
        positions_map.pop(symbol, None)
        _save_state(state)
        await _send(context, cid,
                    f"✅ *Серия отыграна* `{symbol}` {tag} {mode_emoji}\n"
                    f"🧮 Итог серии: *{'+' if series_total >= 0 else ''}{series_total:.2f}$* — "
                    f"сбрасываю мартингейл\n{stats_str}")
        return

    next_series = current_series + 1
    if next_series > max_series:
        series_map[symbol] = 0
        pnl_map.pop(symbol, None)
        positions_map.pop(symbol, None)
        _save_state(state)
        await _send(context, cid,
                    f"🛑 *СЕРИЯ СЛИТА* `{symbol}` ({max_series} шагов) {tag} {mode_emoji}\n"
                    f"📉 {outcome} по {int(close_price)}¢ (вход {entry_cents}¢) → {pnl:+.2f}$\n"
                    f"🧮 Фактический убыток серии: *{series_total:.2f}$*\n"
                    f"♻️ Мартингейл сброшен, дальше — снова базовый лот\n{stats_str}")
        return

    series_map[symbol] = next_series
    pnl_map[symbol] = series_total
    positions_map.pop(symbol, None)
    _save_state(state)

    nxt_stake = compute_stake(c, next_series)
    await _send(context, cid,
                f"🔴 *ШАГ ПРОИГРАН* `{symbol}` {tag} {mode_emoji}\n"
                f"📉 `{slug}` {outcome} по {int(close_price)}¢ (вход {entry_cents}¢) → {pnl:+.2f}$\n"
                f"♻️ Серия {current_series}→{next_series}/{max_series} | "
                f"долг {abs(series_total):.2f}$\n"
                f"🕯 В следующем окне войду за его свечой лотом *${nxt_stake:.2f}*\n"
                + (f"🧭 {ls.md_escape(reason)}\n" if reason else "")
                + stats_str)
    log.info(f"trend: ♻️ {symbol} серия {next_series}/{max_series}, "
             f"следующий лот ${nxt_stake:.2f}")


# ===================== ФОНОВЫЕ ТИКИ =====================
_signal_lock = asyncio.Lock()
_position_lock = asyncio.Lock()


async def scan_for_signal(context):
    """Тик поиска входа (интервал td_check_interval)."""
    if _signal_lock.locked():
        log.debug("trend: предыдущий проход входа ещё идёт, пропускаю тик")
        return
    async with _signal_lock:
        try:
            await _scan_entry_impl(context)
        except Exception as e:
            log.exception(f"trend scan_for_signal error: {e}")


def _boundary_lead(c: dict) -> float:
    """Полоса перед концом окна, в которой оцениваем живую свечу. К lead
    добавляем интервал тика — тик может не успеть попасть в границу."""
    return (max(0.0, _f_num(c, "td_entry_lead_sec", 2.0))
            + max(0.5, _f_num(c, "td_check_interval", 1.0)))


async def _boundary_entry(context, cid, session, c: dict, symbol: str,
                          cur_start: int):
    """Граница окна: оценка ЖИВОЙ свечи текущего окна и решения.

    - Позиции нет → вход в СЛЕДУЮЩЕЕ (ещё не начавшееся) окно по направлению
      свечи — пока цена там стоит ~50¢.
    - Позиция есть, свеча в нашу сторону → держим дальше (до TP или конца).
    - Позиция есть, свеча против → закрыть этот рынок сейчас и сразу открыть
      следующее окно в направлении свечи (лот мартингейла).
    """
    tf = str(c.get("td_timeframe", "5m"))
    dur = ls.TF_SECONDS.get(tf, 300)
    next_start = cur_start + dur
    # Снимок OI на границе — даже если входа не будет: история снапшотов
    # нужна, чтобы сравнить это окно с предыдущим на СЛЕДУЮЩЕЙ границе.
    if _flat_cfg(c)["oi"][0] > 0:
        await _flat_oi_record(symbol, cur_start)
    candle = await _candle_for_window(session, symbol, tf, cur_start, force=True)
    D = ls.resolve_state(candle)
    if not D:
        # Свечи ещё нет (RTDS штормит) — не ставим метку, следующий тик
        # попробует снова; если так до конца окна — сработает фолбэк-вход.
        await _skip_and_note(symbol, "граница: нет данных свечи текущего окна")
        return
    _mark_bd(symbol, cur_start)

    st = _load_state()
    pos = (st.get("positions") or {}).get(symbol)
    if pos:
        if pos.get("outcome") == D:
            set_setting("td_last_scan",
                        f"[{symbol}] граница: свеча {D} в нашу сторону — держим "
                        f"до TP/конца окна")
            return
        if str(c.get("td_salvage_on", "1")) == "1":
            log.info(f"trend: {symbol} свеча у конца окна {D} против "
                     f"{pos.get('outcome')} — закрываем и идём за свечой дальше")
            await _boundary_close(context, cid, session, c, st, symbol, pos, D)
        else:
            set_setting("td_last_scan",
                        f"[{symbol}] граница: свеча {D} против — держим "
                        f"(закрытие при минусе выключено)")
            return
    # Вход в следующее окно (или retry пропущенных попыток — try_cnt).
    ok = await _open_in_window(context, cid, session, c, symbol, next_start, D,
                               candle, src_note=f"Свеча текущего окна у его конца: {D}",
                               sig_start=cur_start)
    if not ok:
        st2 = _load_state()
        burned = int((st2.get("last_window") or {}).get(symbol, -1)) == int(next_start)
        if not burned:
            # Не терминальный пропуск (нет рынка/FAK не принят) — снимаем
            # метку границы, чтобы следующий тик повторил, а после полосы
            # подхватил фолбэк-путь.
            stt = _load_state()
            if int((stt.get("bd_seen") or {}).get(symbol, -1)) == int(cur_start):
                stt["bd_seen"].pop(symbol, None)
                _save_state(stt)


async def _fallback_entry(context, cid, session, c: dict, symbol: str,
                          cur_start: int, age: float):
    """Фолбэк: вход в первые секунды УЖЕ начавшегося окна по последней
    ЗАКРЫТОЙ свече — если граница упущена (не было данных свечи/сбой)."""
    tf = str(c.get("td_timeframe", "5m"))
    dur = ls.TF_SECONDS.get(tf, 300)
    st = _load_state()
    if symbol in (st.get("positions") or {}):
        return
    if int((st.get("last_window") or {}).get(symbol, -1)) == int(cur_start):
        return
    prev_start = cur_start - dur
    # Граница предыдущего окна уже всё решила по этой свече — не дублируем.
    if int((st.get("bd_seen") or {}).get(symbol, -1)) == int(prev_start):
        return
    cndl = await ls.get_window_candle(session, symbol, tf, prev_start)
    D2 = ls.resolve_state(cndl)
    if not D2:
        _mark_window(symbol, cur_start, inc_try=5)
        await _skip_and_note(symbol, f"фолбэк: нет данных свечи {prev_start}")
        return
    await _open_in_window(context, cid, session, c, symbol, cur_start, D2, cndl,
                          src_note=f"Фолбэк по закрытой свече ({D2}), вход "
                                    f"через {int(age)}с после старта окна",
                          sig_start=prev_start)


async def _scan_entry_impl(context):
    if not is_active():
        return
    cid = context.job.data.get("cid") if context.job and context.job.data else None
    c = cfg()
    symbols = get_selected_symbols()
    if not symbols:
        set_setting("td_last_scan", "пары не выбраны — зайди в настройки")
        return
    tf = str(c.get("td_timeframe", "5m"))
    dur = ls.TF_SECONDS.get(tf, 300)
    now = time.time()
    cur_start = int(now // dur) * dur
    time_left = cur_start + dur - now
    age = now - cur_start

    in_boundary = 0 < time_left <= _boundary_lead(c)
    delay = max(0.0, _f_num(c, "td_entry_delay_sec", 0.0))
    window = max(delay + 2.0, _f_num(c, "td_entry_window_sec", 60.0))
    in_fallback = (not in_boundary) and delay <= age <= window
    if not in_boundary and not in_fallback:
        return  # вне полосы решений — спим
    if _flat_enabled(c):
        await _flat_ensure_streams()  # WS-буферы должны знать наши монеты

    async with aiohttp.ClientSession() as session:
        for symbol in symbols:
            try:
                if in_boundary:
                    if int((( _load_state().get("bd_seen") or {}).get(symbol, -1))) == int(cur_start):
                        continue  # эта граница уже оценена
                    await _boundary_entry(context, cid, session, c, symbol, cur_start)
                else:
                    await _fallback_entry(context, cid, session, c, symbol, cur_start, age)
            except Exception as e:
                log.exception(f"trend: {symbol} entry error: {e}")


async def scan_open_position(context):
    """Тик ведения позиций (интервал td_scan_interval).

    ВАЖНО: ведение НЕ зависит от флага активности — если стратегию
    выключили, открытые позиции всё равно доживаются до расчёта окна
    (иначе они остались бы висеть мёртвым грузом). Новые входы при этом
    не открываются (это делает scan_for_signal).
    """
    if _position_lock.locked():
        log.debug("trend: предыдущий проход ведения ещё идёт, пропускаю тик")
        return
    async with _position_lock:
        try:
            await _scan_positions_impl(context)
        except Exception as e:
            log.exception(f"trend scan_open_position error: {e}")


async def _scan_positions_impl(context):
    state = _load_state()
    positions = state.get("positions") or {}
    if not positions:
        return
    cid = context.job.data.get("cid") if context.job and context.job.data else None
    c = cfg()
    async with aiohttp.ClientSession() as session:
        for sym, pos in list(positions.items()):
            try:
                await _manage_position(context, cid, session, c, state, sym, pos)
            except Exception as e:
                log.exception(f"trend: {sym} position error: {e}")


# ===================== СТАТУС =====================
def get_status_text() -> str:
    st = _load_state()
    c = cfg()
    is_demo_mode = get_setting("demo_mode", "0") == "1"
    is_demo_int = 1 if is_demo_mode else 0
    symbols = get_selected_symbols()
    tf = c["td_timeframe"]
    dur = ls.TF_SECONDS.get(tf, 300)
    now = time.time()
    cur_start = int(now // dur) * dur
    age = now - cur_start
    time_left = cur_start + dur - now
    in_boundary = 0 < time_left <= _boundary_lead(c)
    in_fallback = (not in_boundary) and (
        _f_num(c, "td_entry_delay_sec", 0.0) <= age
        <= max(0, _f_num(c, "td_entry_window_sec", 60.0)))

    L = []
    L.append(f"🕯 *ДВИЖЕНИЕ ЗА РЫНКОМ* — "
             f"{'🟢 *ВКЛЮЧЕНА*' if is_active() else '🔴 *ВЫКЛЮЧЕНА*'}"
             f"{' | 🎮 ДЕМО' if is_demo_mode else ' | 💰 РЕАЛ'}")
    L.append(f"⚙️ Версия логики: `{ls.md_escape(STRATEGY_VERSION)}`")
    if symbols:
        L.append(f"💱 Пары: *{', '.join('`' + s + '`' for s in symbols)}* | "
                 f"⏱ ТФ: `{tf}` ({dur // 60} мин)")
    else:
        L.append("💱 Пары: *не выбраны* (открой ⚙️ Настройки)")
    src = "🔗 Chainlink TWAP" if get_candle_source() == "chainlink" else "🟢 Спот Gate.io"
    L.append(f"🕯 Вход ({src}): за *{int(_f_num(c, 'td_entry_lead_sec', 2))}с до конца окна* "
             f"ставим в следующее по его живой свече (цена ~50¢)"
             f"{' ⚡️ решение сейчас' if in_boundary else f' | до конца окна {time_left:.0f}с'}")
    L.append(f"♿️ Фолбэк (упущена граница): {int(_f_num(c, 'td_entry_delay_sec', 0)):g}–"
             f"{_f_num(c, 'td_entry_window_sec', 60):g}с от старта окна по закрытой свече"
             f"{' ✅' if in_fallback else ''}")
    cap = _f_int(c, "td_entry_cap_cents", 0)
    L.append(f"💵 Лот: *${_f_num(c, 'td_base_stake', 5):g}* | ♻️ Мартингейл: "
             f"×{_f_num(c, 'td_martingale_mult', 2):g} за шаг, "
             f"макс. серия *{_f_int(c, 'td_max_series', 5)}* | "
             f"🚦 кэп входа: *{str(cap) + '¢' if cap else 'выкл'}*")
    L.append(f"🏁 TP: *{_f_int(c, 'td_tp_cents', 80)}¢* "
             f"({'отложник, если позволяет минимум, иначе FAK' if c.get('td_tp_mode') == 'auto' else ('всегда лимитка' if c.get('td_tp_mode') == 'limit' else 'всегда FAK')}) | "
             f"🛡 На границе при минусе: "
             f"{'закрыть + сразу следующее окно' if str(c.get('td_salvage_on', '1')) == '1' else 'держим до конца'}")
    _fc = _flat_cfg(c)
    if any(pct > 0 for pct, _m in _fc.values()):
        def _mk(name, pct_mode):
            pct, md = pct_mode
            if pct <= 0:
                return f"{name} ⛔"
            return f"{name} {'<' if md == 'below' else '>'}{pct:g}%"
        L.append("🌫 Против.prev свечи (блок = не входим): "
                 + " | ".join(_mk(n, _fc[k]) for n, k in
                              (("лика", "liq"), ("cvd", "cvd"), ("oi", "oi"))))
    L.append(f"🔁 Тики: вход {c['td_check_interval']}с / позиция {c['td_scan_interval']}с | "
             f"🔒 макс. позиций: {c['td_max_concurrent']} | "
             f"⌛️ ожидание расчёта: {c['td_settle_grace_sec']}с")
    L.append(f"📈 Входов: {_stat(SIGNAL_STATS)} | пропусков: {_stat(SKIP_STATS)} "
             f"(всё — только по ЭТОЙ стратегии)")
    try:
        cl = clp.status()
        L.append(f"📡 Chainlink RTDS: {'✅' if cl.get('connected') else '⚠️'} "
                 f"обновлений {cl.get('updates', 0)}")
    except Exception:
        pass
    L.append(_stats_line(is_demo_int))
    L.append("")

    positions = st.get("positions") or {}
    series_map = st.get("series") or {}
    for sym in symbols:
        pos = positions.get(sym)
        ser = int(series_map.get(sym, 0) or 0)
        debt = abs(get_series_pnl(st, sym))
        if pos:
            tl = max(0, int(float(pos.get("window_end", 0)) - now))
            L.append(f"🔒 `{sym}` *ОТКРЫТА ПОЗИЦИЯ* | серия {ser}/{c['td_max_series']}")
            L.append(f"   🎯 `{pos.get('slug')}` → *{pos.get('outcome')}* @ "
                     f"{pos.get('entry_cents')}¢ | ${pos.get('stake')} "
                     f"({pos.get('shares')} долей, FAK)")
            if pos.get("tp_order_id"):
                L.append(f"   📋 TP-отложник живёт: лимитка на {pos.get('tp_order_price')}¢ "
                         f"(ждём исполнения)")
            L.append(f"   ⏳ До конца окна: *{tl}с*")
        elif ser > 0 or debt > 0:
            nxt = compute_stake(c, ser) if ser > 0 else _f_num(c, "td_base_stake", 5)
            L.append(f"🟢 `{sym}` | ♻️ серия {ser}/{c['td_max_series']} "
                     f"| долг {debt:.2f}$ → след. лот ${nxt:.2f} "
                     f"| вход в следующее окно за его свечой")
        else:
            L.append(f"🟢 `{sym}` свободна | жду конец текущего окна — за "
                     f"{int(_f_num(c, 'td_entry_lead_sec', 2)):g}с до него войду "
                     f"в следующее по его свече")
    if not symbols:
        L.append("📭 *Ни одной пары не выбрано — открой ⚙️ Настройки.*")

    last_scan = get_setting("td_last_scan", "")
    if last_scan:
        L.append("")
        L.append(f"🧾 Последнее событие: {ls.md_escape(last_scan)}")
    L.append("")
    L.append("ℹ️ Система полностью независима от «Каскада ликвидаций»: "
             "свои пары, лоты, серии и статистика. Могут торговать и одну "
             "монету одновременно.")
    return "\n".join(L)


def get_stats_text(is_demo: int | None = None) -> str:
    if is_demo is None:
        is_demo = 1 if get_setting("demo_mode", "0") == "1" else 0
    st = get_trade_stats(is_demo)
    mode = "🎮 ДЕМО" if is_demo else "💰 РЕАЛ"
    L = [f"📊 *Статистика «Движение за рынком»* ({mode})",
         "Учтены только сделки этой стратегии — статистика «Ликвидаций» "
         "не смешивается.",
         ""]
    if not st["total"]:
        L.append("Сделок пока нет. Включи стратегию (🟢) и дождись "
                 "конца текущего окна — вход придёт за ним.")
        return "\n".join(L)
    L.append(f"Всего сделок: *{st['total']}* | 🟢 {st['wins']} | 🔴 {st['losses']} "
             f"| WR *{st['winrate']}%*")
    L.append(f"PnL: *{'+' if st['total_pnl'] >= 0 else ''}{st['total_pnl']}$* | "
             f"Средняя: {st['avg_pnl']}$ | PF: {st['profit_factor']}")
    L.append(f"Средний профит: +{st['avg_win']}$ | Средний убыток: {st['avg_loss']}$")
    L.append(f"Лучшая: +{st['best']}$ | Худшая: {st['worst']}$")
    L.append("")
    L.append(f"*Последние сделки (до {len(st['recent'])}):*")
    for t in st["recent"]:
        pnl = float(t.get("pnl", 0) or 0)
        q = str(t.get("question", ""))[:28]
        L.append(f"{'🟢' if pnl >= 0 else '🔴'} {q} | {t.get('outcome')} "
                 f"{t.get('entry_price')}¢→{t.get('close_price')}¢ | "
                 f"{'+' if pnl >= 0 else ''}{pnl}$")
    return "\n".join(L)
