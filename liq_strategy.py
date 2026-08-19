"""
Стратегия "Каскад ликвидаций" для Polymarket Up/Down.

ВХОД (нужны обе части — каскад и свеча):
  1. Ловим ликвидации с Binance/Bybit/OKX/Gate (WS + публичный REST OKX),
     копим событийный буфер 600с по каждой монете.
  2. Агрегируем за liq_window_sec. Нужен total_usd >= liq_threshold_usd
     и явное доминирование одной стороны.
  3. Смотрим ПОСЛЕДНЮЮ ЗАВЕРШЁННУЮ свечу 5m (спот Gate.io — та же цена,
     по которой Polymarket считает Up/Down):
       свеча DOWN + ликвидируют ЛОНГОВ → входим UP  (контр-трейд)
       свеча UP   + ликвидируют ШОРТОВ → входим DOWN
     Если свеча и каскад смотрят в разные стороны — сигнал пропускаем.
  4. Вход в СЛЕДУЮЩЕЕ окно, по рынку или лимиткой (liq_entry_mode).

ВЫХОД:
  1. Достигнут liq_tp_cents → продаём (лимиткой на TP, а если позиция
     меньше $5 — по рынку).
  2. За liq_new_order_time секунд до конца окна смотрим свечу ТЕКУЩЕГО
     окна, то есть где цена относительно старта рынка:
       • идёт в нашу сторону → НЕ закрываемся досрочно, ждём расчёта
         (раньше бот здесь сливал выигрышные позиции по 40¢);
       • идёт против нас → шаг проигран: спасаем остаток продажей и
         сразу открываем следующее окно в ТУ ЖЕ сторону с мартингейлом.
  3. После конца окна итог определяем по закрытой свече этого окна
     (с перепроверкой ценой рынка). Выигрыш — серия сброшена, проигрыш —
     следующий шаг мартингейла в ту же сторону.

Мартингейл идёт по таймеру внутри серии и не ждёт нового каскада.
"""

import json
import logging
import math
import time
import asyncio
from datetime import datetime, timezone

import aiohttp

import liq_api
import chainlink_price as clp
from database import get_setting, set_setting, add_trade_history, get_trade_statistics

log = logging.getLogger("bot.liq_strategy")


_MD_SPECIALS = ("_", "*", "`", "[", "]")


def md_escape(text) -> str:
    """Экранирует спецсимволы Telegram Markdown в произвольной строке.

    Без этого любой `BTC_USDT`, попавший в текст вне обратных кавычек
    (например, в строке «Последняя проверка: [BTC_USDT] каскад …»),
    открывает курсив, который никогда не закрывается, и Telegram
    отвечает «Can't parse entities: can't find end of the entity».
    """
    out = str(text if text is not None else "")
    for ch in _MD_SPECIALS:
        out = out.replace(ch, "\\" + ch)
    return out


async def _send(context, cid, text, **kwargs):
    """Отправка сообщения с откатом на обычный текст.

    Если в текст всё же просочилась битая Markdown-разметка, сообщение
    уходит без форматирования, а не теряется целиком (раньше из-за этого
    падал весь job и переставали работать кнопки стратегии).
    """
    kwargs.setdefault("parse_mode", "Markdown")
    body = text if len(text) <= 4000 else text[:4000]
    try:
        return await context.bot.send_message(cid, body, **kwargs)
    except Exception as e:
        if "parse entities" not in str(e).lower():
            log.warning(f"send err: {e}")
            return None
        log.warning(f"Markdown битый, шлю как текст: {e}")
        kwargs.pop("parse_mode", None)
        try:
            return await context.bot.send_message(cid, body, **kwargs)
        except Exception as e2:
            log.warning(f"send plain err: {e2}")
            return None

GATE_BASE = liq_api.GATE_BASE

# ===================== ПАРАМЕТРЫ ПО УМОЛЧАНИЮ =====================
DEFAULTS = {
    "liq_active":        "0",
    # Список выбранных монет, JSON-массив вида ["BTC_USDT","ETH_USDT"]
    "liq_symbols":       "[]",
    "liq_timeframe":     "5m",
    "liq_window_sec":    "60",
    "liq_threshold_usd": "150000",
    "liq_check_interval": "5",
    "liq_min_size_usd":  "1000",
    "liq_base_stake":    "1",
    "liq_martingale_mult": "2",
    "liq_entry_price_cents": "51",
    "liq_scan_interval": "1",
    "liq_new_order_time": "3",
    "liq_max_series":    "5",
    # Take-profit в центах: когда цена нашего исхода достигает этого значения,
    # бот ставит sell-лимитку ровно на TP и закрывает позицию. Если цена так и
    # не дошла до TP, за liq_new_order_time до конца окна бот закрывает по рынку
    # (текущая страховка). По умолчанию 90 — фиксируем почти всю прибыль.
    "liq_tp_cents":      "90",
    # Сколько последних сделок показывать в блоке «Последние сделки» статуса.
    "liq_recent_count":  "20",
    # Тип входа: "market" (по текущей лучшей цене) или "limit" (по entry_price_cents).
    "liq_entry_mode":    "market",
    # Что делать, если заданный лот меньше минимального ордера рынка
    # (Polymarket: обычно 5 shares, то есть ~$2.50 при цене 50¢):
    #   "skip" — не входить и написать, какой лот нужен (по умолчанию);
    #   "bump" — войти минимально возможным размером рынка.
    "liq_min_size_mode": "skip",
    # Источник цены для свечей:
    #   "chainlink" — TWAP Chainlink через публичный RTDS Polymarket
    #                 (тот же поток, по которому рынок и рассчитывается);
    #   "gate_spot" — спот Gate.io (запасной вариант).
    "liq_candle_source": "chainlink",
    # За сколько секунд до конца сигнальной свечи перепроверяем её
    # направление перед входом в следующее окно.
    "liq_entry_confirm_sec": "2",
}

MIN_SIZE_MODES = ("skip", "bump")
CANDLE_SOURCES = ("chainlink", "gate_spot")

# Минимум ордера на Polymarket CLOB задаётся в ДОЛЯХ (shares), а не в
# долларах: обычно 5 shares. В деньгах это зависит от цены — 5 shares по
# 50¢ = $2.50, по 20¢ = $1.00. Раньше константа трактовалась как «минимум
# $5», из-за чего размер лота считался неверно.
POLY_MIN_ORDER_SHARES = 5.0
# Дополнительный порог по сумме ордера (CLOB не принимает совсем мелочь).
POLY_MIN_NOTIONAL_USD = 1.0
# Оставлено для обратной совместимости со старым кодом/настройками.
POLY_MIN_ORDER_USD = 5.0

# Режимы входа (для валидации и UI).
ENTRY_MODES = ("market", "limit")

# Все монеты, которые бот предлагает в чек-листе меню настроек.
AVAILABLE_SYMBOLS = [
    "BTC_USDT",
    "ETH_USDT",
    "SOL_USDT",
    "XRP_USDT",
    "DOGE_USDT",
    "BNB_USDT",
]

TF_SECONDS = {"5m": 300, "15m": 900, "1h": 3600}
GATE_INTERVAL = {"5m": "5m", "15m": "15m", "1h": "1h"}

ASSET_MAP = {
    "BTC_USDT":  "btc",
    "ETH_USDT":  "eth",
    "SOL_USDT":  "sol",
    "XRP_USDT":  "xrp",
    "DOGE_USDT": "doge",
    "BNB_USDT":  "bnb",
}

# Красивые названия монет для отображения в статистике и логах.
# Используется в get_trade_stats и формировании строки «Монета» в сообщениях.
ASSET_DISPLAY = {
    "BTC_USDT":  "Bitcoin",
    "ETH_USDT":  "Ethereum",
    "SOL_USDT":  "Solana",
    "XRP_USDT":  "XRP",
    "DOGE_USDT": "Dogecoin",
    "BNB_USDT":  "BNB",
}

# Эмодзи-бейджи для бирж — чтобы в больших блоках ликвидаций сразу
# было видно, какая биржа поймала какой каскад. Порядок здесь
# определяет порядок вывода, если у бирж равный объём.
EXCHANGE_EMOJI = {
    "Binance": "🟡",
    "Bybit":   "🟠",
    "OKX":     "🔵",
    "Gate.io": "🟢",
}

# Базовый URL для ссылки на Polymarket market.
# Slug вида "btc-updown-5m-1234567890" → URL event:
#   https://polymarket.com/event/<asset>-updown-<tf>-<ts>
POLYMARKET_BASE_URL = "https://polymarket.com/event"


def _polymarket_url(slug: str) -> str:
    """Собрать прямую ссылку на Polymarket по нашему slug'у."""
    return f"{POLYMARKET_BASE_URL}/{slug}"


def _exchange_badge(name: str) -> str:
    """Вернуть эмодзи-бейдж биржи + её имя (например '🟡 Binance')."""
    e = EXCHANGE_EMOJI.get(name, "⚪")
    return f"{e} {name}"


def _exchange_short(name: str) -> str:
    """Короткая подпись биржи (для плотных строк) — только эмодзи + первая буква имени."""
    e = EXCHANGE_EMOJI.get(name, "⚪")
    short_name = "Gate" if name == "Gate.io" else name
    return f"{e}{short_name}"


def asset_display_name(symbol: str) -> str:
    """Короткое читаемое имя монеты (для логов и статистики)."""
    return ASSET_DISPLAY.get(symbol, symbol)


def _format_trade_question(symbol: str, raw_question: str | None) -> str:
    """Формирует человеко-читаемое имя события для trade_history.

    Полимаркет обычно возвращает что-то вроде "Bitcoin Up or Down" / "Ethereum
    Up or Down" — это и сохраняем. Если поле пустое (старые данные, либо маркет
    только что появился и groupItemTitle не подгрузился), подставляем имя по
    словарю. Это нужно, чтобы статистика корректно показывала BTC/ETH/XRP, а
    не "Bitcoin" для всех сделок.
    """
    base = asset_display_name(symbol)
    q = (raw_question or "").strip()
    # Если пришёл осмысленный question от Polymarket и он содержит «Up or Down»,
    # оставляем как есть (там будет имя монеты).
    if q and "Up or Down" in q:
        return q
    if q and "UpDown" in q:
        return f"{base} Up or Down"
    # Иначе формируем сами
    if q and base.lower() not in q.lower():
        return f"{base} {q}"
    return f"{base} Up or Down"

# Буфер событий как в другом боте: symbol -> list[events] за последние 600с
_events_buffer: dict[str, list] = {}
_BUFFER_TTL = 600  # сек, сколько храним события для агрегации


def cfg():
    out = {}
    for k, v in DEFAULTS.items():
        out[k] = get_setting(k, v)
    return out


def cfg_int(key):
    return int(float(cfg().get(key, DEFAULTS[key])))


def is_active() -> bool:
    return get_setting("liq_active", "0") == "1"


def set_active(v: bool):
    set_setting("liq_active", "1" if v else "0")


def get_param(key):
    return get_setting(key, DEFAULTS.get(key, ""))


def set_param(key, value):
    set_setting(key, str(value))


# ===================== СОСТОЯНИЕ (ПЕРСИСТЕНТНОЕ) =====================
def _empty_state() -> dict:
    """Свежее состояние: серии, позиции и отложенные (неподтверждённые) входы."""
    return {"series": {}, "positions": {}, "pending": {}}


def _load_state() -> dict:
    raw = get_setting("liq_state", "")
    if not raw:
        return _empty_state()
    try:
        st = json.loads(raw)
        # Миграция со старого формата {series:int, position:...}
        if not isinstance(st, dict):
            return _empty_state()
        # Если это старый формат с int series и одной position — мигрируем
        if "series" in st and not isinstance(st.get("series"), dict):
            old_sym = get_setting("liq_symbol", "") or "BTC_USDT"
            old_series = int(st.get("series", 0) or 0)
            old_pos = st.get("position")
            new_st = {"series": {old_sym: old_series}, "positions": {}, "pending": {}}
            if old_pos and isinstance(old_pos, dict):
                # старая позиция была привязана к одной монете
                sym = old_pos.get("symbol", old_sym)
                new_st["positions"][sym] = old_pos
            return new_st
        # Нормализация для нового формата
        st.setdefault("series", {})
        st.setdefault("positions", {})
        st.setdefault("pending", {})
        if not isinstance(st["series"], dict):
            st["series"] = {}
        if not isinstance(st["positions"], dict):
            st["positions"] = {}
        if not isinstance(st["pending"], dict):
            st["pending"] = {}
        return st
    except Exception:
        return _empty_state()


def _save_state(state: dict):
    set_setting("liq_state", json.dumps(state))


def reset_state():
    """Полный сброс: очищаем все серии и позиции по всем монетам."""
    _save_state(_empty_state())
    _events_buffer.clear()
    set_setting("liq_last_agg", "")
    set_setting("liq_last_events", "")
    set_setting("liq_last_check_ts", "")


def _normalize_symbol(s: str) -> str | None:
    """Приводит свободный ввод к формату XXX_USDT. None если не похоже на символ."""
    if not s or not isinstance(s, str):
        return None
    t = s.strip().upper()
    if not t:
        return None
    # Уже XXX_USDT
    if "_USDT" in t:
        # уберём лишние подчёркивания в начале
        t = t.replace(" ", "").replace("/", "_").replace("-", "_")
        if "_USDT" in t:
            base = t.split("_USDT", 1)[0].strip("_")
            base = "".join(ch for ch in base if ch.isalnum())
            if not base:
                return None
            return f"{base}_USDT"
    # Формат XXXUSDT без подчёркивания
    if t.endswith("USDT") and t != "USDT":
        base = t[:-4]
        base = "".join(ch for ch in base if ch.isalnum())
        if base:
            return f"{base}_USDT"
        return None
    # Голый тикер BTC -> BTC_USDT
    if t.isalnum() and 2 <= len(t) <= 20:
        return f"{t}_USDT"
    return None


def get_selected_symbols() -> list:
    """Возвращает список выбранных монет (нормализованный, без дубликатов)."""
    raw = get_setting("liq_symbols", DEFAULTS["liq_symbols"]) or "[]"
    try:
        items = json.loads(raw)
        if not isinstance(items, list):
            return []
    except Exception:
        return []
    # нормализация и де-дупликация с сохранением порядка
    seen = set()
    out = []
    for it in items:
        norm = _normalize_symbol(it) if isinstance(it, str) else None
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def set_selected_symbols(symbols: list):
    """Записывает список выбранных монет в БД как JSON. Каждый элемент нормализуется."""
    cleaned = []
    seen = set()
    for it in symbols or []:
        norm = _normalize_symbol(it) if isinstance(it, str) else None
        if not norm or norm in seen:
            continue
        seen.add(norm)
        cleaned.append(norm)
    set_setting("liq_symbols", json.dumps(cleaned))


def has_open_position(symbol: str) -> bool:
    return symbol in (_load_state().get("positions") or {})


def get_series(symbol: str) -> int:
    return int((_load_state().get("series") or {}).get(symbol, 0) or 0)


def limit_min_shares(market_info: dict | None) -> float:
    """Минимум ЛИМИТНОГО ордера в долях (обычно 5)."""
    val = 0.0
    if market_info:
        try:
            val = float(market_info.get("min_shares")
                        or market_info.get("min_size") or 0)
        except (TypeError, ValueError):
            val = 0.0
    return val if val > 0 else POLY_MIN_ORDER_SHARES


def sell_shares(pos: dict, shares: float, price_cents: int) -> tuple[bool, str, object]:
    """Продажа долей с учётом типа ордера.

    Лимитная продажа (GTC) требует минимум 5 долей, поэтому мелкие
    позиции (например, 2 доли после входа на $1) закрываются рыночным
    FAK — у него ограничение только по сумме ($1).

    Возвращает (успех, режим, ответ_биржи).
    """
    import polymarket_trading as pt

    price_cents = max(1, min(99, int(price_cents or 0)))
    price = price_cents / 100.0
    notional = shares * price
    min_shares = float(pos.get("min_shares") or POLY_MIN_ORDER_SHARES)

    if notional < POLY_MIN_NOTIONAL_USD:
        # Даже рыночный ордер такую мелочь не примет.
        return False, "dust", None

    if shares >= min_shares:
        try:
            res = pt.place_order(pos["token_id"], "SELL", price, shares,
                                 allow_min_bump=False)
            if isinstance(res, dict) and not res.get("error"):
                return True, "limit", res
            log.warning(f"liq: sell-limit не прошёл: {res}")
        except Exception as e:
            log.warning(f"liq: sell-limit exception: {e}")

    # Мелкая позиция или лимитка не прошла — уходим рыночным FAK.
    try:
        res = pt.place_market_order(pos["token_id"], "SELL", shares,
                                    order_type="FAK")
        if isinstance(res, dict) and not res.get("error"):
            return True, "market", res
        log.warning(f"liq: sell-market не прошёл: {res}")
        return False, "market", res
    except Exception as e:
        log.warning(f"liq: sell-market exception: {e}")
        return False, "market", None


def get_min_size_mode() -> str:
    """skip — пропускать вход, если лот меньше минимума рынка; bump — доливать."""
    v = str(get_setting("liq_min_size_mode", DEFAULTS["liq_min_size_mode"]) or "").strip().lower()
    return v if v in MIN_SIZE_MODES else "skip"


def plan_order(stake_usd: float, entry_cents: int, market_info: dict | None,
               order_kind: str = "market") -> dict:
    """Считает реальный размер ордера с учётом минимумов Polymarket.

    Минимумы у биржи РАЗНЫЕ и зависят от типа ордера:

      • рыночный (FOK/FAK) — не ложится в стакан, минимум $1 по сумме.
        Ровно так работает кнопка Market на сайте: можно войти на $1
        по любой цене;
      • лимитный (GTC/GTD) — ложится в стакан, минимум 5 ДОЛЕЙ. В деньгах
        это зависит от цены: 5 долей по 50¢ = $2.50, по 20¢ = $1.00.

    Возвращает словарь:
      shares      — сколько долей примерно получим,
      cost        — во сколько это обойдётся в $,
      min_shares  — минимум в долях для этого типа ордера,
      min_cost    — тот же минимум в долларах по цене входа,
      below_min   — правда ли, что заданный лот меньше минимума,
      kind        — тип ордера, для которого считали.
    """
    price = max(0.01, min(0.99, entry_cents / 100.0))
    kind = "limit" if str(order_kind).lower() == "limit" else "market"

    if kind == "limit":
        min_shares = 0.0
        if market_info:
            try:
                min_shares = float(market_info.get("min_shares")
                                   or market_info.get("min_size") or 0)
            except (TypeError, ValueError):
                min_shares = 0.0
        if min_shares <= 0:
            min_shares = POLY_MIN_ORDER_SHARES
        # Лимитка тоже должна стоить не меньше минимальной суммы.
        min_shares = max(min_shares, POLY_MIN_NOTIONAL_USD / price)
    else:
        # Рыночный: ограничение только по сумме.
        min_shares = POLY_MIN_NOTIONAL_USD / price

    want_shares = stake_usd / price
    below_min = want_shares < min_shares - 1e-9
    shares = min_shares if below_min else want_shares
    return {
        "shares": round(shares, 4),
        "cost": round(shares * price, 2),
        "min_shares": round(min_shares, 4),
        "min_cost": round(min_shares * price, 2),
        "want_shares": round(want_shares, 4),
        "below_min": below_min,
        "kind": kind,
    }


def get_entry_mode() -> str:
    """Текущий режим входа: 'market' или 'limit'. По умолчанию 'market'."""
    mode = (get_setting("liq_entry_mode", DEFAULTS["liq_entry_mode"]) or "market").strip().lower()
    return mode if mode in ENTRY_MODES else "market"


def any_active_position() -> bool:
    """Есть ли открытая позиция по ЛЮБОЙ монете (используется как глобальный стоп)."""
    st = _load_state()
    return bool((st.get("positions") or {}) or (st.get("pending") or {}))


def any_active_series() -> bool:
    """Есть ли активная (не нулевая) серия мартингейла по любой монете."""
    series = (_load_state().get("series") or {})
    return any(int(v or 0) > 0 for v in series.values())


def can_open_for(symbol: str) -> tuple[bool, str]:
    """
    Можно ли открыть новую сделку по конкретной монете.

    Правила:
    - По монете, у которой уже есть открытая позиция — нельзя (защита от дублей).
    - По монете с активной серией (series > 0) и БЕЗ открытой позиции —
      МОЖНО. Это продолжение мартингейла: предыдущий шаг проиграл, мы
      ждём следующий сигнал по той же монете с увеличенной ставкой.
    - По любой ДРУГОЙ монете — нельзя, пока по этой монете идёт активная
      серия или открыта позиция (глобальный стоп по депозиту).
    """
    st = _load_state()
    positions = st.get("positions") or {}
    series = st.get("series") or {}
    pending = st.get("pending") or {}

    if symbol in positions:
        return False, "уже есть открытая позиция по этой монете"
    if symbol in pending:
        return False, "по этой монете уже ждём подтверждения сигнала"

    # По этой монете идёт серия (без открытой позиции) — разрешаем продолжение
    if int(series.get(symbol, 0) or 0) > 0:
        return True, ""

    # Иначе проверяем глобальные блокировки
    for other_sym, pos in positions.items():
        if other_sym != symbol:
            return False, f"открыта позиция по `{other_sym}`"
    for other_sym in pending:
        if other_sym != symbol:
            return False, f"ждём подтверждения сигнала по `{other_sym}`"
    for other_sym, ser in series.items():
        if other_sym != symbol and int(ser or 0) > 0:
            return False, f"идёт серия мартингейла по `{other_sym}`"
    return True, ""


def global_trade_locked() -> tuple[bool, str]:
    """
    Глобальная блокировка новых сделок по депозиту.
    Возвращает (locked: bool, reason: str).

    Используется для статуса и для запрета входа по «чужим» монетам.
    Точная проверка для конкретной монеты — в can_open_for().
    """
    if any_active_position():
        return True, "есть открытая позиция"
    if any_active_series():
        return True, "идёт активная серия мартингейла"
    return False, ""


# ===================== HELPERS =====================
def make_power_bar(total_usd: float, threshold_usd: float) -> str:
    """
    Визуальная шкала силы каскада из другого бота.
    1 кружок = threshold, 10 кружков = threshold × 10
    """
    if threshold_usd <= 0:
        return ""
    ratio = total_usd / threshold_usd
    dots = min(int(math.ceil(ratio)), 10)
    dots = max(dots, 1)
    bar = ""
    for i in range(1, dots + 1):
        if i <= 3:
            bar += "🟡"
        elif i <= 6:
            bar += "🟠"
        elif i <= 9:
            bar += "🔴"
        else:
            bar += "💥"
    empty = 10 - dots
    bar += "⚫" * empty
    return f"{bar} ({dots}/10)"


def _fmt_usd_compact(v: float) -> str:
    try:
        v = float(v)
        if v >= 1_000_000:
            return f"${v/1_000_000:.2f}M"
        if v >= 1000:
            return f"${v/1000:.1f}k"
        return f"${v:.0f}"
    except:
        return f"${v}"


def _time_ago(ts: float) -> str:
    try:
        diff = time.time() - float(ts)
        if diff < 60:
            return f"{int(diff)}с назад"
        if diff < 3600:
            return f"{int(diff//60)}м {int(diff%60)}с назад"
        return f"{int(diff//3600)}ч назад"
    except:
        return ""


def _cascade_age_inline(events: list, now: float) -> str:
    """Короткая строка «когда случился каскад» для блока объяснения входа.

    Возвращает что-то вроде:
      «12с назад — 30с назад (5 ордеров)»
    Используется внутри блока «ПОЧЕМУ ИМЕННО ЭТОТ ВХОД».
    """
    if not events:
        return "нет данных по времени"
    try:
        ts_min = min(float(e.get("time", 0) or 0) for e in events)
        ts_max = max(float(e.get("time", 0) or 0) for e in events)
        return f"{_time_ago(ts_max)} — {_time_ago(ts_min)} ({len(events)} ордеров)"
    except Exception:
        return "не удалось вычислить"


def _load_json_setting(key: str, default):
    raw = get_setting(key, "")
    if not raw:
        return default
    try:
        return json.loads(raw)
    except:
        return default


def _save_last_liquidation(symbol: str, agg: dict, events: list, candle: str | None, oi_data: dict | None = None):
    """Сохраняет последний каскад по конкретной монете. Для статуса показываем последний из всех."""
    now = time.time()
    data = {
        "ts": now,
        "symbol": symbol,
        "total_usd": agg.get("total_usd", 0),
        "long_usd": agg.get("long_liq_usd", 0),
        "short_usd": agg.get("short_liq_usd", 0),
        "long_count": agg.get("long_count", 0),
        "short_count": agg.get("short_count", 0),
        "dominant": agg.get("dominant", "NEUTRAL"),
        "by_exchange": agg.get("by_exchange", {}),
        "candle": candle,
        "window_sec": int(float(get_setting("liq_window_sec", "60"))),
        "threshold": float(get_setting("liq_threshold_usd", "150000")),
        "oi_total": (oi_data or {}).get("total", 0),
        "oi_breakdown": oi_data or {},
    }
    # power_bar и impact сразу считаем для статуса
    try:
        data["power_bar"] = make_power_bar(data["total_usd"], data["threshold"])
        if data["oi_total"] > 0:
            data["impact_pct"] = (data["total_usd"] / data["oi_total"]) * 100
        else:
            data["impact_pct"] = 0
    except:
        data["power_bar"] = ""
        data["impact_pct"] = 0

    # Кладём per-symbol снапшот для детального статуса
    per_sym = _load_json_setting("liq_last_agg_by_symbol", {})
    if not isinstance(per_sym, dict):
        per_sym = {}
    per_sym[symbol] = data
    set_setting("liq_last_agg_by_symbol", json.dumps(per_sym))

    # Глобальный "последний" = самый свежий среди всех символов
    latest_symbol = None
    latest_ts = 0
    for s, d in per_sym.items():
        ts = float(d.get("ts", 0) or 0)
        if ts >= latest_ts:
            latest_ts = ts
            latest_symbol = s
    if latest_symbol and latest_symbol in per_sym:
        set_setting("liq_last_agg", json.dumps(per_sym[latest_symbol]))
    else:
        set_setting("liq_last_agg", json.dumps(data))

    # События — тоже держим per-symbol
    try:
        sorted_events = sorted(events, key=lambda x: x.get("time", 0), reverse=True)[:12]
        simplified = []
        for e in sorted_events:
            simplified.append({
                "exchange": e.get("exchange"),
                "symbol": e.get("symbol"),
                "direction": e.get("direction"),
                "usd": e.get("usd_value"),
                "price": e.get("price"),
                "time": e.get("time"),
            })
        per_evt = _load_json_setting("liq_last_events_by_symbol", {})
        if not isinstance(per_evt, dict):
            per_evt = {}
        per_evt[symbol] = simplified
        set_setting("liq_last_events_by_symbol", json.dumps(per_evt))

        # Глобальный "последний" = свежайший по времени
        latest_sym_evt = None
        latest_ts_evt = 0
        for s, ev in per_evt.items():
            if not ev:
                continue
            ts = max(float(e.get("time", 0) or 0) for e in ev)
            if ts >= latest_ts_evt:
                latest_ts_evt = ts
                latest_sym_evt = s
        if latest_sym_evt and per_evt.get(latest_sym_evt):
            set_setting("liq_last_events", json.dumps(per_evt[latest_sym_evt]))
    except Exception as ex:
        log.warning(f"save events err {ex}")
    set_setting("liq_last_check_ts", str(now))


def _get_trade_stats(is_demo: int):
    """Собирает статистику по сделкам ликвидационной стратегии.

    Сделки фильтруются так: берём только те, у которых в slug есть
    «updown-<tf>-» (это наши 5m/15m/1h окна, в отличие от произвольных
    пользовательских рынков). Раньше фильтр был по слову «Up or Down» в
    question, из-за чего ВСЕ сделки сводились к Bitcoin — теперь фильтруем
    по slug, поэтому BTC/ETH/XRP/SOL/DOGE/BNB отображаются корректно.

    recent_count читается из настройки liq_recent_count (по умолчанию 20).
    """
    try:
        trades = get_trade_statistics(is_demo)
    except Exception:
        trades = []
    # Фильтр по slug: наши окна имеют вид "<asset>-updown-<tf>-<ts>".
    # Слагов других стратегий этот шаблон не имеют.
    def _is_liq(t):
        slug = str(t.get("slug", "") or "")
        return "-updown-" in slug
    strat_trades = [t for t in trades if _is_liq(t)]
    use = strat_trades if strat_trades else trades
    if not use:
        return {
            "total": 0, "wins": 0, "losses": 0, "winrate": 0,
            "total_pnl": 0, "avg_pnl": 0, "avg_win": 0, "avg_loss": 0,
            "profit_factor": 0, "best": 0, "worst": 0,
            "recent": [], "strat_total": len(strat_trades), "all_total": len(trades),
        }
    pnls = [float(t.get("pnl", 0)) for t in use]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    total = len(pnls)
    win_cnt = len(wins)
    total_pnl = sum(pnls)
    avg_pnl = total_pnl / total if total else 0
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 0
    sum_win = sum(wins)
    sum_loss = abs(sum(losses)) if losses else 0
    pf = sum_win / sum_loss if sum_loss > 0 else (999 if sum_win > 0 else 0)
    # recent_count: либо из настройки, либо дефолт 20.
    try:
        recent_n = int(float(get_setting("liq_recent_count", DEFAULTS["liq_recent_count"])))
    except Exception:
        recent_n = 20
    recent_n = max(1, min(50, recent_n))  # защита от мусорных значений
    recent = sorted(use, key=lambda x: x.get("timestamp", 0), reverse=True)[:recent_n]
    return {
        "total": total, "wins": win_cnt, "losses": len(losses),
        "winrate": round(win_cnt / total * 100, 1) if total else 0,
        "total_pnl": round(total_pnl, 2), "avg_pnl": round(avg_pnl, 2),
        "avg_win": round(avg_win, 2), "avg_loss": round(avg_loss, 2),
        "profit_factor": round(pf, 2) if pf != 999 else 999,
        "best": round(max(pnls), 2) if pnls else 0,
        "worst": round(min(pnls), 2) if pnls else 0,
        "recent": recent, "strat_total": len(strat_trades), "all_total": len(trades),
    }


def _candle_source_line() -> str:
    """Откуда берём свечи и живы ли данные Chainlink."""
    src = get_candle_source()
    if src != "chainlink":
        return "📊 Свеча: *спот Gate.io* (Polymarket считает по Chainlink — возможны расхождения)"
    try:
        st = clp.status()
    except Exception:
        return "📊 Свеча: *Chainlink TWAP* (статус недоступен)"
    age = st.get("age_sec")
    if st.get("connected") and age is not None and age < 30:
        return (f"📊 Свеча: *Chainlink TWAP* через Polymarket RTDS 🟢 "
                f"(тиков {st['updates']}, пар {st['symbols']}, обновление {int(age)}с назад)")
    if st.get("connected"):
        return "📊 Свеча: *Chainlink TWAP* 🟡 подключено, ждём тики (пока фолбэк на спот Gate.io)"
    return "📊 Свеча: *Chainlink TWAP* 🔴 нет связи — временно считаем по споту Gate.io"


def _ws_status_line() -> str:
    """Строка диагностики WS-источников ликвидаций.

    Bybit / Binance / Gate.io работают через публичные WebSocket-стримы
    (REST у Binance и Gate требует API-ключей с подписью), OKX — через
    публичный REST, поэтому его тут нет.
    """
    try:
        health = liq_api.ws_health()
    except Exception:
        return "📡 WS: нет данных"

    titles = {"bybit": "Bybit", "binance": "Binance", "gate": "Gate.io"}
    parts = []
    for key, title in titles.items():
        h = health.get(key) or {}
        mark = "🟢" if h.get("connected") else "🔴"
        age = h.get("age_sec")
        if age is None:
            when = "событий не было"
        elif age < 90:
            when = f"{int(age)}с назад"
        else:
            when = f"{int(age // 60)}м назад"
        parts.append(f"{mark} {title} ({h.get('buffered', 0)}, {when})")
    return "📡 WS ликвидаций: " + " | ".join(parts)


def get_status_text() -> str:
    st = _load_state()
    c = cfg()
    is_demo_mode = get_setting("demo_mode", "0") == "1"
    is_demo_int = 1 if is_demo_mode else 0
    symbols = get_selected_symbols()

    lines = []
    lines.append(f"🤖 *АЛГОТОРГОВЛЯ Ликвидаций* — {'🟢 *ВКЛЮЧЕНА*' if is_active() else '🔴 *ВЫКЛЮЧЕНА*'}")
    if symbols:
        lines.append(f"💱 Пары: *{', '.join(f'`{s}`' for s in symbols)}* | ⏱ ТФ: `{c['liq_timeframe']}`")
    else:
        lines.append(f"💱 Пары: *не выбраны* (открой ⚙️ Настройки) | ⏱ ТФ: `{c['liq_timeframe']}`")
    lines.append(f"🎮 Режим: {'ДЕМО' if is_demo_mode else 'РЕАЛ'}")
    entry_mode = get_entry_mode()
    entry_mode_pretty = "🚀 Рыночный (по лучшей цене)" if entry_mode == "market" else "📋 Лимитный (по цене из настроек)"
    lines.append(f"🎯 Тип входа: *{entry_mode_pretty}*")
    lines.append(f"💥 Порог: *${float(c['liq_threshold_usd']):,.0f}* | 🪟 Окно: *{c['liq_window_sec']}с* (буфер 600с) | 🔎 Мин.ликв: *${c['liq_min_size_usd']}*")
    min_mode_txt = ("⏭ пропускать вход" if get_min_size_mode() == "skip"
                    else "⬆️ заходить минимумом рынка")
    _em = get_entry_mode()
    if _em == "market":
        min_lot_txt = f"рыночный вход: минимум *${POLY_MIN_NOTIONAL_USD:.2f}*"
    else:
        _mc = 0.05 * float(c['liq_entry_price_cents'])
        min_lot_txt = (f"лимитный вход: минимум 5 долей = "
                       f"*${_mc:.2f}* при {c['liq_entry_price_cents']}¢")
    lines.append(f"💵 Лот: *{c['liq_base_stake']}$* | ✖️ Мартин: *x{c['liq_martingale_mult']}* | 🎯 Лимит-цена: *{c['liq_entry_price_cents']}¢* | 🧮 Макс.серия: *{c['liq_max_series']}*")
    lines.append(f"🚧 {min_lot_txt} | если лот меньше: *{min_mode_txt}*")
    lines.append(f"🏁 TP: *{c['liq_tp_cents']}¢* | ⏰ Страховка: *{c['liq_new_order_time']}с* до конца окна | 🕘 В списке: *{c['liq_recent_count']}* сделок")
    lines.append(f"🔁 Чек: {c['liq_check_interval']}с | 👁 Скан: {c['liq_scan_interval']}с")
    lines.append(_candle_source_line())
    lines.append(f"⏳ Перепроверка свечи перед входом: за *{get_entry_confirm_sec()}с* до её закрытия")
    lines.append(_ws_status_line())
    lines.append("")

    # Глобальный стоп-индикатор в статусе
    locked, lock_reason = global_trade_locked()
    if locked:
        # Если по какой-то монете идёт активная серия — это не «полный» стоп,
        # а только по чужим монетам. По самой монете с серией бот продолжит
        # мартингейл на следующем сигнале.
        active_series_syms = [
            s for s, v in (st.get("series") or {}).items() if int(v or 0) > 0
        ]
        if active_series_syms and not (st.get("positions") or {}):
            # Серия идёт, открытых позиций нет — бот ждёт продолжения мартингейла
            extra = " Продолжение мартингейла разрешено по: " + ", ".join(f"`{s}`" for s in active_series_syms)
            lines.append(
                f"🔒 *Стоп по чужим монетам*: {md_escape(lock_reason)}.{extra}"
            )
        else:
            lines.append(
                f"🔒 *Глобальный СТОП по депозиту*: {md_escape(lock_reason)}. "
                f"Новых сделок не открываем ни по одной монете до закрытия текущего цикла."
            )
        lines.append("")

    # === Состояние по каждой выбранной монете ===
    positions = st.get("positions") or {}
    series_map = st.get("series") or {}
    if symbols:
        for sym in symbols:
            pos = positions.get(sym)
            ser = int(series_map.get(sym, 0) or 0)
            buf = len(_events_buffer.get(sym, []))
            status_emoji = "🔒" if pos else "🟢"
            if pos and pos.get("window_end"):
                eta = int(pos["window_end"] - time.time())
                mode_str = "🎮 ДЕМО" if pos.get("is_demo", 0) == 1 else "💰 РЕАЛ"
                potential = round(pos["stake"] * (100 - pos["entry_cents"]) / pos["entry_cents"], 2)
                lines.append(f"{status_emoji} `{sym}` *ОТКРЫТА ПОЗИЦИЯ* ({mode_str}) | серия {ser}/{c['liq_max_series']}")
                lines.append(f"   🎯 `{pos['slug']}` → *{pos['outcome']}*")
                lines.append(f"   💵 Ставка: *{pos['stake']}$* ({pos.get('shares','?')} shares @ {pos['entry_cents']}¢) | Потенциал: +{potential}$")
                lines.append(f"   ⏳ До конца окна: *{max(eta,0)}с* | Серия шага: {pos.get('series',0)}")
                end_dt = datetime.fromtimestamp(pos["window_end"], tz=timezone.utc).strftime("%H:%M:%S UTC")
                lines.append(f"   🕐 Закрытие окна: {end_dt}")
                agg_snap = pos.get("agg_snapshot", {})
                if agg_snap:
                    lines.append(f"   💥 Каскад на входе: {agg_snap.get('dominant','?')} ${agg_snap.get('total_usd',0):,.0f} свеча {pos.get('candle','?')}")
                lines.append(f"   🧠 Буфер: {buf} событий за 600с")
            else:
                lines.append(f"{status_emoji} `{sym}` свободна | серия {ser}/{c['liq_max_series']} | буфер {buf} за 600с")
            lines.append("")
    else:
        lines.append("📭 *Не выбрано ни одной пары — открой ⚙️ Настройки.*")
        lines.append("")

    # === Отложенные входы (ждут подтверждения свечой) ===
    pending = st.get("pending") or {}
    if pending:
        lines.append("⏸ *Ожидают подтверждения свечой:*")
        for sym, req in pending.items():
            left = int((req.get("signal_window_end") or 0) - time.time())
            lines.append(
                f"   `{sym}` → *{req.get('outcome','?')}* | свеча на сигнале "
                f"*{req.get('signal_candle','?')}* | до проверки {max(left, 0)}с"
            )
        lines.append("")

    # === Последний каскад (любой монеты) ===
    agg = _load_json_setting("liq_last_agg", None)
    last_check_ts_raw = get_setting("liq_last_check_ts", "")
    if agg:
        sym_disp = agg.get("symbol", "")
        ts = agg.get("ts", 0)
        ago = _time_ago(ts)
        total = agg.get("total_usd", 0)
        long_usd = agg.get("long_usd", 0)
        short_usd = agg.get("short_usd", 0)
        long_c = agg.get("long_count", 0)
        short_c = agg.get("short_count", 0)
        dom = agg.get("dominant", "NEUTRAL")
        candle = agg.get("candle", "?")
        by_ex = agg.get("by_exchange", {})
        dom_emoji = "🔴" if dom == "LONG" else "🟢" if dom == "SHORT" else "⚪️"
        power_bar = agg.get("power_bar", make_power_bar(total, agg.get("threshold", 150000)))
        impact_pct = agg.get("impact_pct", 0)
        oi_total = agg.get("oi_total", 0)

        ratio = total / agg.get("threshold", 1) if agg.get("threshold", 0) > 0 else 0
        if ratio >= 5:
            strength = "🔴🔴🔴 МОЩНЫЙ КАСКАД"
        elif ratio >= 2:
            strength = "🔴🔴 Сильный"
        else:
            strength = "🟡 Каскад"

        sym_label = f" `{sym_disp}`" if sym_disp else ""
        lines.append(f"💥 *Последний каскад* {sym_label} ({ago}): {dom_emoji} *{dom}* | {strength}")
        lines.append(f"   {power_bar}")
        lines.append(f"   💰 Всего: *${total:,.0f}* за {agg.get('window_sec',60)}с (порог ${agg.get('threshold',0):,.0f}) | Свеча: *{candle}*")
        lines.append(f"   🔴 LONG ликв (лонги): *${long_usd:,.0f}* ({long_c}) | 🟢 SHORT ликв (шорты): *${short_usd:,.0f}* ({short_c})")
        if oi_total > 0:
            if impact_pct >= 1.0:
                impact_label = "🔴🔴🔴 КРИТИЧЕСКИЙ"
            elif impact_pct >= 0.5:
                impact_label = "🔴🔴 Очень сильный"
            elif impact_pct >= 0.1:
                impact_label = "🔴 Сильный"
            elif impact_pct >= 0.05:
                impact_label = "🟡 Заметный"
            else:
                impact_label = "⚪ Слабый"
            lines.append(f"   💪 Импакт: *{impact_pct:.3f}%* от OI ({_fmt_usd_compact(oi_total)}) — {impact_label}")
        if by_ex:
            lines.append(f"   📡 *По биржам:*")
            for ex_name in ["Bybit", "Gate.io", "OKX"]:
                if ex_name not in by_ex:
                    continue
                ex = by_ex[ex_name]
                l_usd = ex.get("long_usd", 0)
                s_usd = ex.get("short_usd", 0)
                l_cnt = ex.get("long_count", 0)
                s_cnt = ex.get("short_count", 0)
                lines.append(f"     • {md_escape(ex_name)}: ${l_usd+s_usd:,.0f} (L ${l_usd:,.0f} x{l_cnt} / S ${s_usd:,.0f} x{s_cnt})")
            for ex_name, ex in by_ex.items():
                if ex_name in ["Bybit", "Gate.io", "OKX"]:
                    continue
                l_usd = ex.get("long_usd", 0)
                s_usd = ex.get("short_usd", 0)
                lines.append(f"     • {md_escape(ex_name)}: ${l_usd+s_usd:,.0f}")
        lines.append("")
    else:
        last_scan = get_setting("liq_last_scan", "")
        if last_scan:
            lines.append(f"🔎 Последняя проверка: {md_escape(last_scan)}")
            lines.append("")
        elif last_check_ts_raw:
            try:
                ago = _time_ago(float(last_check_ts_raw))
                lines.append(f"🔎 Последняя проверка: {ago}")
                lines.append("")
            except:
                pass
        else:
            lines.append("🔎 Пока нет данных о ликвидациях (ждём первый скан).")
            lines.append("")

    # События по всем монетам
    per_evt = _load_json_setting("liq_last_events_by_symbol", {})
    if isinstance(per_evt, dict) and per_evt:
        # покажем 3 свежайших события по каждой активной монете
        any_shown = False
        for sym in symbols:
            evs = per_evt.get(sym) or []
            if not evs:
                continue
            if not any_shown:
                lines.append("🧾 *Последние ликвидации (топ-3 на монету):*")
                any_shown = True
            lines.append(f"   ▸ `{sym}`:")
            for e in evs[:3]:
                ex = e.get("exchange", "?")
                direction = e.get("direction", "?")
                usd = e.get("usd", 0)
                price = e.get("price", 0)
                t = e.get("time", 0)
                ago = _time_ago(t)
                dir_emoji = "🔴 LONG→" if direction == "LONG" else "🟢 SHORT→" if direction == "SHORT" else direction
                lines.append(f"     {dir_emoji} {md_escape(ex)} {_fmt_usd_compact(usd)} @ {price} ({ago})")
        if any_shown:
            lines.append("")

    stats = _get_trade_stats(is_demo_int)
    if stats["total"] > 0:
        pf_str = "∞" if stats["profit_factor"] == 999 else str(stats["profit_factor"])
        lines.append(f"📈 *Статистика ({'ДЕМО' if is_demo_mode else 'РЕАЛ'} — стратегийных {stats['strat_total']} из {stats['all_total']} всего):*")
        lines.append(f"   Всего: *{stats['total']}* | ✅ Вин: *{stats['wins']}* | ❌ Луз: *{stats['losses']}* | 🎯 WR: *{stats['winrate']}%*")
        lines.append(f"   💰 PnL: *{'+' if stats['total_pnl']>=0 else ''}{stats['total_pnl']}$* | Сред: {stats['avg_pnl']}$ | PF: {pf_str}")
        lines.append(f"   📊 Ср.вин +{stats['avg_win']}$ | Ср.луз {stats['avg_loss']}$ | Best +{stats['best']}$ | Worst {stats['worst']}$")
        if stats["recent"]:
            recent_n = len(stats["recent"])
            lines.append(f"   🕘 Последние сделки ({recent_n}):")
            # Формируем краткое имя монеты из slug: «btc-updown-5m-123» → BTC
            def _short_coin(slug: str) -> str:
                try:
                    base = str(slug).split("-updown-", 1)[0]
                    return base.upper() if base else "?"
                except Exception:
                    return "?"
            for tr in stats["recent"]:
                pnl = float(tr.get("pnl", 0))
                q = md_escape(str(tr.get("question", ""))[:22])
                outcome = tr.get("outcome", "?")
                ts = float(tr.get("timestamp", 0))
                ago = _time_ago(ts)
                coin = _short_coin(tr.get("slug", ""))
                emoji = "🟢" if pnl > 0 else "🔴"
                lines.append(f"     {emoji} {'+' if pnl>0 else ''}{pnl}$ | {coin} {outcome} | {q} | {ago}")
        lines.append("")
    else:
        lines.append("📈 *Статистика пуста — сделок ещё не было.*")
        lines.append(f"   (Режим {'ДЕМО' if is_demo_mode else 'РЕАЛ'})")
        lines.append("")

    lines.append(
        "💡 Пока по одной монете открыта позиция или идёт серия мартингейла, по другим "
        "монетам вход блокируется — продолжение мартингейла по той же монете разрешено. "
        "Вход по сигналу: свеча DOWN + ликвидации лонгов → UP, свеча UP + ликвидации "
        "шортов → DOWN. Выход: по TP, либо держим до расчёта, если свеча идёт в нашу "
        "сторону. Свеча берётся со спота Gate.io — это та же цена, по которой Polymarket "
        "считает Up/Down."
    )
    return "\n".join(lines)


# ===================== СВЕЧА =====================
# Кэш свечей, чтобы не ходить на API на каждом тике скана.
# key=(symbol, interval) -> {"ts": float, "candles": [dict], "src": str}
_spot_candle_cache: dict = {}
_CANDLE_CACHE_TTL = 2.0

# Порог «дожи»: движение меньше 0.005% считаем неопределённым.
_DOJI_PCT = 0.00005

# Порядок полей в ответе /spot/candlesticks Gate.io:
#   [t, quote_volume, close, high, low, open, base_volume, window_closed]
# (тот же порядок t,v,c,h,l,o, что и в WS-канале spot.candlesticks).
# Раньше код читал row[2] как open и row[5] как close — то есть
# open и close были ПЕРЕПУТАНЫ МЕСТАМИ, и направление свечи всегда
# определялось наоборот. Это ломало весь контр-трейд.
_SPOT_IDX = {"t": 0, "close": 2, "high": 3, "low": 4, "open": 5}

# Автопроверка порядка полей: сравниваем close текущей свечи с last
# из /spot/tickers. Если API когда-нибудь поменяет порядок — сами
# это заметим и переключимся, а не будем молча торговать наоборот.
_spot_order_checked = False


async def _verify_spot_field_order(session, symbol: str, rows: list):
    """Один раз за запуск сверяет close последней свечи с ценой из тикера."""
    global _SPOT_IDX, _spot_order_checked
    if _spot_order_checked or not rows:
        return
    _spot_order_checked = True
    try:
        data = await liq_api.fetch_json(
            session, "https://api.gateio.ws/api/v4/spot/tickers",
            params={"currency_pair": symbol},
        )
        if not data or not isinstance(data, list):
            return
        last = float(data[0].get("last") or 0)
        if last <= 0:
            return
        row = rows[-1]
        as_close = abs(float(row[2]) - last)
        as_open = abs(float(row[5]) - last)
        if as_open < as_close:
            log.warning(
                "liq: Gate.io поменял порядок полей свечи — "
                "переключаюсь на open=row[2], close=row[5]"
            )
            _SPOT_IDX = {"t": 0, "open": 2, "high": 3, "low": 4, "close": 5}
        else:
            log.info("liq: порядок полей свечи Gate.io подтверждён (close=row[2], open=row[5])")
    except Exception as e:
        log.debug(f"spot field order check err: {e}")


def _parse_spot_row(row, dur: int, now: float) -> dict | None:
    """Строка массива /spot/candlesticks → словарь свечи."""
    try:
        t = float(row[_SPOT_IDX["t"]])
        o = float(row[_SPOT_IDX["open"]])
        c = float(row[_SPOT_IDX["close"]])
        h = float(row[_SPOT_IDX["high"]])
        low = float(row[_SPOT_IDX["low"]])
    except (TypeError, ValueError, IndexError, KeyError):
        return None
    if o <= 0 or c <= 0:
        return None
    closed = (t + dur) <= now
    # У Gate.io в 8-м поле бывает флаг закрытия окна — он точнее часов.
    try:
        if len(row) >= 8:
            flag = str(row[7]).lower()
            if flag in ("true", "false"):
                closed = flag == "true"
    except Exception:
        pass
    return {"t": t, "open": o, "close": c, "high": h, "low": low,
            "closed": closed, "src": "gateio_spot"}


def _parse_fut_row(row, dur: int, now: float) -> dict | None:
    """Свеча фьючерсов Gate.io — объект {t, v, c, h, l, o}."""
    if not isinstance(row, dict):
        return None
    try:
        t = float(row.get("t") or 0)
        o = float(row.get("o") or 0)
        c = float(row.get("c") or 0)
        h = float(row.get("h") or o)
        low = float(row.get("l") or o)
    except (TypeError, ValueError):
        return None
    if o <= 0 or c <= 0:
        return None
    return {"t": t, "open": o, "close": c, "high": h, "low": low,
            "closed": (t + dur) <= now, "src": "gateio_fut"}


def candle_state(candle: dict | None) -> str | None:
    """UP / DOWN / None (дожи) по одной свече.

    Именно этим бот отвечает на вопрос «цена выше или ниже старта окна»:
    open свечи 5m совпадает со стартом окна Polymarket Up/Down (обе сетки
    выровнены по UTC), а close — это либо текущая цена (окно идёт), либо
    цена на момент расчёта (окно закрыто).
    """
    if not candle:
        return None
    o = candle.get("open") or 0
    c = candle.get("close") or 0
    if o <= 0 or c <= 0:
        return None
    if abs(c - o) / o < _DOJI_PCT:
        return None
    return "UP" if c > o else "DOWN"


async def get_candles(session, symbol: str, timeframe: str = "5m",
                      limit: int = 5, force: bool = False) -> list:
    """Свечи Gate.io SPOT (приоритет) или FUTURES (fallback), по возрастанию t.

    Спот — та же цена, по которой Polymarket рассчитывает Up/Down,
    поэтому решения бота совпадают с графиком рынка.
    """
    if not symbol or not symbol.endswith("_USDT"):
        return []
    interval = GATE_INTERVAL.get(timeframe, "5m")
    dur = TF_SECONDS.get(timeframe, 300)
    now = time.time()

    cache_key = (symbol, interval)
    cached = _spot_candle_cache.get(cache_key)
    if (not force and cached and
            (now - cached["ts"]) < _CANDLE_CACHE_TTL and
            len(cached.get("candles") or []) >= min(limit, 2)):
        return cached["candles"]

    # 1. Gate.io SPOT
    try:
        data = await liq_api.fetch_json(
            session, "https://api.gateio.ws/api/v4/spot/candlesticks",
            params={"currency_pair": symbol, "interval": interval,
                    "limit": max(limit, 3)},
        )
        if data and isinstance(data, list) and data:
            await _verify_spot_field_order(session, symbol, data)
            candles = [
                x for x in (_parse_spot_row(r, dur, now) for r in data) if x
            ]
            if candles:
                candles.sort(key=lambda x: x["t"])
                _spot_candle_cache[cache_key] = {
                    "ts": now, "candles": candles, "src": "gateio_spot"}
                return candles
    except Exception as e:
        log.debug(f"gateio_spot candles err: {e}")

    # 2. Fallback: Gate.io USDT-M FUTURES
    try:
        data = await liq_api.fetch_json(
            session, f"{GATE_BASE}/candlesticks",
            params={"contract": symbol, "interval": interval,
                    "limit": max(limit, 3)},
        )
        if data and isinstance(data, list) and data:
            candles = [
                x for x in (_parse_fut_row(r, dur, now) for r in data) if x
            ]
            if candles:
                candles.sort(key=lambda x: x["t"])
                _spot_candle_cache[cache_key] = {
                    "ts": now, "candles": candles, "src": "gateio_fut"}
                return candles
    except Exception as e:
        log.debug(f"gateio_fut candles err: {e}")

    return []


async def get_prev_candle(session, symbol: str, timeframe: str = "5m") -> dict | None:
    """Последняя ЗАВЕРШЁННАЯ свеча — на неё смотрим при входе по сигналу."""
    dur = TF_SECONDS.get(timeframe, 300)

    if get_candle_source() == "chainlink":
        try:
            prev_start = _window_bounds(timeframe, time.time(), -1)[0]
            cndl = clp.get_window_candle(symbol, prev_start,
                                         prev_start + dur, timeframe)
            if cndl:
                return cndl
        except Exception as e:
            log.debug(f"chainlink prev candle err: {e}")

    candles = await get_candles(session, symbol, timeframe, limit=4)
    for cndl in reversed(candles):
        if cndl.get("closed"):
            return cndl
    # Флага закрытия нет — берём предпоследнюю (последняя ещё рисуется)
    if len(candles) >= 2:
        return candles[-2]
    return None


def get_candle_source() -> str:
    v = str(get_setting("liq_candle_source",
                        DEFAULTS["liq_candle_source"]) or "").strip().lower()
    return v if v in CANDLE_SOURCES else "chainlink"


def resolve_state(candle: dict | None) -> str | None:
    """Направление окна по правилу Polymarket: close >= open → UP.

    В отличие от candle_state здесь НЕТ зоны дожи: рынок Up/Down
    рассчитывается строго «больше либо равно», ничьей не бывает.
    """
    if not candle:
        return None
    o = candle.get("open") or 0
    c = candle.get("close") or 0
    if o <= 0 or c <= 0:
        return None
    return "UP" if c >= o else "DOWN"


async def get_window_candle(session, symbol: str, timeframe: str,
                            window_start: float, force: bool = False) -> dict | None:
    """Свеча КОНКРЕТНОГО окна Polymarket (по времени старта окна).

    Пока окно идёт — это «живая» свеча: её open = цена на старте рынка,
    close = текущая цена. Так бот понимает, выше он старта или ниже.
    После закрытия окна та же свеча даёт итог, по которому Polymarket
    рассчитывает Up/Down.
    """
    if not window_start:
        return None

    dur = TF_SECONDS.get(timeframe, 300)

    # 1. Chainlink TWAP через RTDS Polymarket — тот же поток, по которому
    #    рынок и рассчитывается. Спот Gate.io здесь регулярно расходится:
    #    у него бывает дожи там, где на Polymarket явная свеча вниз.
    if get_candle_source() == "chainlink":
        try:
            cndl = clp.get_window_candle(symbol, window_start,
                                         window_start + dur, timeframe)
            if cndl:
                return cndl
        except Exception as e:
            log.debug(f"chainlink window candle err: {e}")

    candles = await get_candles(session, symbol, timeframe, limit=5, force=force)
    target = int(window_start)
    for cndl in candles:
        if int(cndl["t"]) == target:
            return cndl
    return None


async def get_candle_direction(
    session: aiohttp.ClientSession,
    symbol: str,
    timeframe: str = "5m",
):
    """Направление последней ЗАВЕРШЁННОЙ свечи: "UP" / "DOWN" / None.

    Оставлено с прежней сигнатурой — используется при разборе сигнала
    и в статусе.
    """
    return candle_state(await get_prev_candle(session, symbol, timeframe))


def _compare_candle(open_price: float, close_price: float, source: str = "") -> str | None:
    """Совместимость со старым кодом: сравнение open/close."""
    return candle_state({"open": open_price, "close": close_price})


# ===================== SLUG =====================
def _window_bounds(timeframe: str, now_ts: float, offset_windows: int = 0):
    dur = TF_SECONDS.get(timeframe, 300)
    start = (int(now_ts) // dur) * dur + offset_windows * dur
    return start, start + dur


def build_updown_slug(symbol: str, timeframe: str, now_ts: float, offset_windows: int = 0) -> str:
    asset = ASSET_MAP.get(symbol, "btc")
    dur_key = timeframe
    start, _ = _window_bounds(timeframe, now_ts, offset_windows)
    return f"{asset}-updown-{dur_key}-{start}"


def pick_entry_window(timeframe: str, now_ts: float, min_left_ratio: float = 0.5) -> int:
    """Какое окно брать для входа: 0 — текущее, 1 — следующее.

    После расчёта рынка новое окно уже идёт несколько секунд, и заходить
    в него нормально. А вот если от окна осталось меньше половины —
    входить поздно, берём следующее.
    """
    dur = TF_SECONDS.get(timeframe, 300)
    left = _window_bounds(timeframe, now_ts, 0)[1] - now_ts
    return 0 if left >= dur * min_left_ratio else 1


def seconds_left_in_window(timeframe: str, now_ts: float) -> float:
    dur = TF_SECONDS.get(timeframe, 300)
    return _window_bounds(timeframe, now_ts, 0)[1] - now_ts


# ===================== ОСНОВНОЙ ЦИКЛ =====================
async def scan_for_signal(context):
    """
    Сканируем каждую выбранную монету.

    Глобальный стоп по депозиту: пока по одной монете открыта позиция или
    идёт активная серия мартингейла, по другим монетам вход блокируется.
    Но продолжение мартингейла по той же монете (series > 0, позиция
    закрыта после проигрыша) — РАЗРЕШЕНО. Иначе серия бы обрывалась
    на втором шаге, что полностью убивает смысл мартингейла.

    Решение по каждой монете принимает can_open_for(symbol):
    - по монете с активной серией и без открытой позиции → можно;
    - по любой другой монете, пока series/position по этой монете активны → нельзя;
    - если нет ни открытых позиций, ни активных серий → можно всем.
    """
    if not is_active():
        return
    cid = context.job.data.get("cid")
    c = cfg()
    symbols = get_selected_symbols()
    if not symbols:
        return
    # Подписываем WS всех бирж (Bybit/Binance/Gate) на нужные монеты
    try:
        liq_api.set_symbols(symbols)
    except Exception as e:
        log.debug(f"set_symbols err: {e}")

    # Подстраховка: если частый джоб не запущен, подтверждаем и здесь.
    try:
        await process_pending(context)
    except Exception as e:
        log.exception(f"process_pending err: {e}")

    state = _load_state()
    async with aiohttp.ClientSession() as session:
        for symbol in symbols:
            allowed, why = can_open_for(symbol)
            if not allowed:
                # Для статуса фиксируем, по какой монете блокировка, но продолжаем
                # обновлять буферы/агрегацию по остальным.
                set_setting(
                    "liq_last_scan",
                    f"[{symbol}] 🔒 стоп: {why}",
                )
                # Если по этой монете серия идёт — мы должны продолжать её
                # слушать (буфер должен пополняться), но _look_for_signal с
                # enter=True не откроет сделку. Вызываем с enter=False, чтобы
                # хотя бы агрегация обновлялась.
                if "серия" in why or "позиция" in why:
                    await _look_for_signal(context, cid, session, c, state, symbol, enter=False)
                continue
            # Разрешено: ищем сигнал, можем войти (в т.ч. продолжить мартингейл)
            await _look_for_signal(context, cid, session, c, state, symbol, enter=True)


async def scan_open_position(context):
    """Проверяем все открытые позиции и отложенные входы (по всем монетам)."""
    if not is_active():
        return
    cid = context.job.data.get("cid")
    c = cfg()

    # Этот джоб самый частый (liq_scan_interval, обычно 1-2с), поэтому
    # именно он ловит последние секунды сигнальной свечи.
    try:
        await process_pending(context)
    except Exception as e:
        log.exception(f"process_pending err: {e}")

    state = _load_state()
    positions = state.get("positions") or {}
    if not positions:
        return
    async with aiohttp.ClientSession() as session:
        for sym, pos in list(positions.items()):
            await _check_open_position(context, cid, session, c, state, sym, pos)


async def _look_for_signal(context, cid, session, c, state, symbol, enter=True):
    """
    Поиск сигнала по одной монете:
    - буфер 600с для каждой монеты независимо
    - агрегация за window_sec
    - не блокируем по свече

    Если enter=False — только собирает буферы/агрегацию и обновляет статус,
    но не открывает сделку. Используется при активном глобальном стопе,
    чтобы данные для статуса оставались свежими.
    """
    global _events_buffer
    window_sec = int(float(c["liq_window_sec"]))
    threshold = float(c["liq_threshold_usd"])
    min_usd = float(c["liq_min_size_usd"])
    tf = c["liq_timeframe"]

    # 1. Получить новые ликвидации
    new_events = await liq_api.get_all_liquidations(session, symbol, min_usd=min_usd)

    # 2. Буфер 600с
    if symbol not in _events_buffer:
        _events_buffer[symbol] = []
    if new_events:
        _events_buffer[symbol].extend(new_events)

    cutoff = time.time() - _BUFFER_TTL
    _events_buffer[symbol] = [e for e in _events_buffer[symbol] if e.get("time", 0) >= cutoff]

    if not _events_buffer[symbol]:
        set_setting("liq_last_scan", f"[{symbol}] буфер пуст, нет ликвидаций за 600с")
        set_setting("liq_last_check_ts", str(time.time()))
        return

    # 3. Агрегация
    agg = liq_api.aggregate_liquidations(_events_buffer[symbol], window_sec=window_sec)

    # 4. Свеча и OI (информативно)
    # Сохраняем источник свечи в кэш (gateio_spot / gateio_fut), чтобы
    # показать пользователю в логах и сообщении — откуда бот взял цену.
    candle = await get_candle_direction(session, symbol, tf)
    # Имя источника — берём из кэша (если есть) для диагностики
    candle_src = ""
    try:
        cache_key = (symbol, GATE_INTERVAL.get(tf, "5m"))
        cc = _spot_candle_cache.get(cache_key)
        if cc:
            candle_src = cc.get("src", "")
    except Exception:
        pass
    # Если в кэше ничего нет, но _compare_candle вернул валидное значение
    # напрямую (например, из кэшированного вызова) — пробуем второй источник.
    if not candle_src and candle in ("UP", "DOWN"):
        # Это означает, что кэш истёк, но get_candle_direction вернул
        # валидное направление — предположим spot, т.к. он приоритетный.
        candle_src = "gateio_spot"
    # Сохраняем источник цены свечи в agg для последующего логирования
    # Делаем это ВСЕГДА (даже если свеча doji и candle=None), чтобы в
    # сообщениях и логах пользователь видел, откуда была взята цена.
    agg["_candle_src"] = candle_src or "none"

    oi_data = {}
    try:
        oi_data = await liq_api.get_multi_oi(session, symbol)
    except Exception as e:
        log.debug(f"OI fetch err {e}")
        oi_data = {"total": 0}

    _save_last_liquidation(symbol, agg, _events_buffer[symbol], candle, oi_data)

    if agg["total_usd"] < threshold:
        set_setting(
            "liq_last_scan",
            f"[{symbol}] каскад ${agg['total_usd']:,.0f} ниже порога ${threshold:,.0f} | {agg['dominant']} | свеча {candle} | буфер {len(_events_buffer[symbol])}",
        )
        return
    if agg["dominant"] == "NEUTRAL":
        set_setting(
            "liq_last_scan",
            f"[{symbol}] каскад ${agg['total_usd']:,.0f} нейтрально LONG ${agg['long_liq_usd']:,.0f} vs SHORT ${agg['short_liq_usd']:,.0f} | свеча {candle}",
        )
        return

    # 5. Выбор стороны: контр-трейд, но ТОЛЬКО при согласии свечи и каскада.
    #
    #   свеча DOWN + ликвидируют ЛОНГОВ  → цену продавили вниз → ставим UP
    #   свеча UP   + ликвидируют ШОРТОВ  → цену вынесли вверх  → ставим DOWN
    #
    # Если свеча и каскад смотрят в разные стороны (например, свеча вниз,
    # но выносят шортистов) — это не разворотная ситуация, а продолжение
    # тренда. Раньше бот в таком случае всё равно входил против свечи и
    # регулярно проигрывал. Теперь такой сигнал пропускается.
    dominant = agg["dominant"]
    if candle == "DOWN" and dominant == "LONG":
        outcome = "UP"
        log.info(f"Signal {symbol} ${agg['total_usd']:.0f} свеча DOWN + ликвидации LONG ({candle_src}) → UP")
    elif candle == "UP" and dominant == "SHORT":
        outcome = "DOWN"
        log.info(f"Signal {symbol} ${agg['total_usd']:.0f} свеча UP + ликвидации SHORT ({candle_src}) → DOWN")
    elif candle is None:
        set_setting(
            "liq_last_scan",
            f"[{symbol}] каскад {dominant} ${agg['total_usd']:,.0f} — свеча нейтральна (дожи), вход пропущен",
        )
        log.info(f"Signal {symbol}: свеча дожи ({candle_src}) — пропускаю")
        return
    else:
        set_setting(
            "liq_last_scan",
            f"[{symbol}] каскад {dominant} ${agg['total_usd']:,.0f} + свеча {candle} — "
            f"нет разворотной связки, вход пропущен",
        )
        log.info(
            f"Signal {symbol}: свеча {candle} и каскад {dominant} не совпали "
            f"({candle_src}) — пропускаю"
        )
        return

    # Если вызвали с enter=False — только логируем каскад, сделку не открываем.
    # Используется, когда по этой монете стоп (например, она не в can_open_for),
    # но мы хотим, чтобы буфер и статус обновлялись.
    if not enter:
        set_setting(
            "liq_last_scan",
            f"[{symbol}] каскад {agg['dominant']} ${agg['total_usd']:,.0f} — стоп, ждём закрытия цикла",
        )
        return

    # Финальная защита: проверяем can_open_for ещё раз непосредственно
    # перед входом — между решением scan_for_signal и записью позиции в
    # state могло что-то измениться (гонка задач, _check_open_position
    # мог сбросить серию и т.п.).
    allowed_now, reason_now = can_open_for(symbol)
    if not allowed_now:
        set_setting(
            "liq_last_scan",
            f"[{symbol}] каскад {agg['dominant']} ${agg['total_usd']:,.0f} — стоп ({reason_now})",
        )
        return

    set_setting(
        "liq_last_scan",
        f"[{symbol}] СИГНАЛ {agg['dominant']} ${agg['total_usd']:,.0f} свеча {candle} → {outcome} ✅ буфер {len(_events_buffer[symbol])}",
    )
    # Вход не делаем прямо сейчас: сначала дождёмся конца ТЕКУЩЕЙ свечи и
    # перепроверим её направление (см. process_pending).
    await _register_pending(context, cid, session, c, state, symbol, outcome,
                            agg, candle, oi_data)


def get_entry_confirm_sec() -> int:
    try:
        v = int(float(get_setting("liq_entry_confirm_sec",
                                  DEFAULTS["liq_entry_confirm_sec"])))
    except (TypeError, ValueError):
        v = 2
    return max(1, min(60, v))


async def _register_pending(context, cid, session, c, state, symbol, outcome,
                            agg, candle, oi_data=None):
    """Ставит вход в очередь до конца сигнальной свечи.

    Каскад ликвидаций происходит ВНУТРИ идущей свечи, и до её закрытия она
    ещё может перекраситься. Если это случилось — ожидаемый откат уже
    состоялся внутри самой сигнальной свечи, и входить в следующее окно
    против неё поздно. Поэтому решение откладывается до последних секунд
    свечи, а там перепроверяется (process_pending).
    """
    tf = c["liq_timeframe"]
    now = time.time()
    win_start, win_end = _window_bounds(tf, now, 0)

    pending = state.setdefault("pending", {})
    pending[symbol] = {
        "symbol": symbol,
        "outcome": outcome,
        "signal_candle": candle,          # направление на момент сигнала
        "signal_window_start": win_start,
        "signal_window_end": win_end,
        "created_ts": now,
        "agg": agg,
        "oi": oi_data or {},
        "cid": cid,
        "tf": tf,
    }
    _save_state(state)

    left = int(win_end - now)
    log.info(
        f"liq: ⏸ {symbol} сигнал {agg['dominant']} свеча {candle} → {outcome}; "
        f"жду конца свечи ({left}с) для перепроверки"
    )
    await _send(
        context, cid,
        f"⏸ *Сигнал принят, ждём конца свечи* `{symbol}`\n"
        f"💥 Каскад: *{agg['dominant']}* ${agg['total_usd']:,.0f} | свеча сейчас *{candle}*\n"
        f"🎯 План: войти *{outcome}* в следующее окно\n"
        f"⏳ Перепроверю направление свечи за {get_entry_confirm_sec()}с до её закрытия "
        f"(осталось {left}с). Если свеча перекрасится — вход отменю.",
    )


async def process_pending(context):
    """Подтверждает или отменяет отложенные входы в конце сигнальной свечи."""
    if not is_active():
        return
    state = _load_state()
    pending = state.get("pending") or {}
    if not pending:
        return

    confirm_sec = get_entry_confirm_sec()
    now = time.time()
    c = cfg()

    async with aiohttp.ClientSession() as session:
        for symbol, req in list(pending.items()):
            tf = req.get("tf") or c["liq_timeframe"]
            dur = TF_SECONDS.get(tf, 300)
            # Границу сигнального окна всегда выравниваем по сетке окон:
            # из неё строится slug следующего рынка, ошибаться нельзя.
            win_start = int(_window_bounds(
                tf, req.get("signal_window_start") or now, 0)[0])
            target_start = win_start + dur
            # Момент перепроверки берём из заявки (в бою совпадает с границей).
            win_end = req.get("signal_window_end") or target_start
            cid = req.get("cid")

            # Ещё рано — свеча не дорисована.
            if now < win_end - confirm_sec:
                continue

            # Слишком поздно: следующее окно уже прошло больше половины.
            if now >= win_end + dur * 0.5:
                log.info(f"liq: ⌛ {symbol} отложенный вход протух, отменяю")
                pending.pop(symbol, None)
                _save_state(state)
                await _send(context, cid,
                            f"⌛ Отменяю отложенный вход `{symbol}`: "
                            f"момент входа упущен.")
                continue

            # Свеча ликвидаций в её последние секунды (или уже закрытая)
            cndl = await get_window_candle(session, symbol, tf, win_start,
                                           force=True)
            state_now = resolve_state(cndl) if cndl else None
            src = (cndl or {}).get("src", "?")
            delta_pct = 0.0
            if cndl and cndl.get("open"):
                delta_pct = (cndl["close"] - cndl["open"]) / cndl["open"] * 100

            signal_candle = req.get("signal_candle")
            outcome = req.get("outcome")

            if state_now is None:
                log.warning(f"liq: {symbol} нет данных по свече для подтверждения")
                if now < win_end + 5:
                    continue
                pending.pop(symbol, None)
                _save_state(state)
                await _send(context, cid,
                            f"⚠️ Отменяю вход `{symbol}`: нет данных по свече "
                            f"для перепроверки.")
                continue

            # === Свеча перекрасилась — откат уже случился, входить поздно ===
            if state_now != signal_candle:
                pending.pop(symbol, None)
                _save_state(state)
                log.info(
                    f"liq: 🚫 {symbol} свеча перекрасилась {signal_candle} → "
                    f"{state_now} ({delta_pct:+.3f}%, {src}) — вход {outcome} отменён"
                )
                set_setting(
                    "liq_last_scan",
                    f"[{symbol}] свеча перекрасилась {signal_candle}→{state_now} — вход отменён",
                )
                await _send(
                    context, cid,
                    f"🚫 *Вход отменён* `{symbol}`\n"
                    f"🕯 Свеча ликвидаций закрылась *{state_now}* "
                    f"({delta_pct:+.3f}%), а на сигнале была *{signal_candle}*\n"
                    f"↩️ Откат, ради которого мы шли в *{outcome}*, уже произошёл "
                    f"внутри этой же свечи — следующее окно пропускаем.",
                )
                continue

            # === Свеча подтвердилась — входим в следующее окно ===
            pending.pop(symbol, None)
            _save_state(state)

            allowed, why = can_open_for(symbol)
            if not allowed:
                log.info(f"liq: {symbol} подтверждение есть, но вход закрыт: {why}")
                continue

            log.info(
                f"liq: ✅ {symbol} свеча подтвердилась ({state_now}, "
                f"{delta_pct:+.3f}%, {src}) — вхожу {outcome}"
            )
            state = _load_state()
            await _enter_trade(context, cid, session, c, state, symbol,
                               outcome, req.get("agg") or {}, signal_candle,
                               req.get("oi") or {},
                               confirm_note=(f"свеча закрылась {state_now} "
                                             f"({delta_pct:+.3f}%) — сигнал подтверждён"),
                               target_window_start=target_start)


async def _enter_trade(context, cid, session, c, state, symbol, outcome, agg, candle,
                       oi_data=None, confirm_note: str = "",
                       target_window_start: float | None = None):
    import polymarket_trading as pt

    now = time.time()
    tf = c["liq_timeframe"]
    dur = TF_SECONDS.get(tf, 300)

    # Целевое окно: то, что начинается сразу за сигнальной свечой. Передаём
    # явно, потому что подтверждение приходит на самой границе окон и
    # арифметика «текущее + 1» может промахнуться на целое окно.
    if target_window_start:
        win_start = int(target_window_start)
    else:
        win_start = int(_window_bounds(tf, now, offset_windows=1)[0])
    win_end = win_start + dur
    slug = f"{ASSET_MAP.get(symbol, 'btc')}-updown-{tf}-{win_start}"

    if now >= win_end:
        log.warning(f"liq_strategy: окно {slug} уже закрылось, пропускаю вход")
        return

    info = pt.get_event_markets(slug)
    if not info or not info.get("markets"):
        log.warning(f"liq_strategy: рынок {slug} не найден, пропускаю сигнал {symbol}")
        try:
            await _send(
                context, cid,
                f"⚠️ Рынок `{slug}` не найден на Polymarket (ещё не сгенерирован). Пропускаю сигнал `{symbol}` {outcome}.",
                parse_mode="Markdown",
            )
        except Exception:
            pass
        return

    m = info["markets"][0]
    token_id = m["token_yes"] if outcome == "UP" else m["token_no"]

    series = int((state.get("series") or {}).get(symbol, 0) or 0)
    base_stake = float(c["liq_base_stake"])
    mult = float(c["liq_martingale_mult"])
    stake_usd = round(base_stake * (mult ** series), 4)

    # === Выбор цены входа по режиму ===
    entry_mode = get_entry_mode()
    limit_price_cents = int(float(c["liq_entry_price_cents"]))
    if not 1 <= limit_price_cents <= 99:
        log.error("liq_strategy: invalid entry price %s", limit_price_cents)
        return

    if entry_mode == "market":
        # Рыночный: берём текущую лучшую цену (best ask) по нужному исходу.
        # Если в ответе нет цены (0), отступаем к лимитной цене из настроек.
        best_cents = int(m.get("price_yes") if outcome == "UP" else m.get("price_no")) or 0
        if best_cents <= 0:
            best_cents = limit_price_cents
        # На Polymarket цены внутри одной шкалы — но лимитный ордер исполнится,
        # только если наша цена >= лучшей. Накинем +1¢, чтобы перебить ask.
        entry_cents = max(1, min(99, best_cents + 1))
    else:
        entry_cents = limit_price_cents

    # === Размер позиции ===
    # Минимум ордера Polymarket задан в ДОЛЯХ (обычно 5 shares), поэтому в
    # деньгах он зависит от цены: 5 shares по 49¢ — это $2.45, а не $1.
    # Раньше бот отправлял 2 доли на $1, а polymarket_trading молча
    # поднимал размер до 5 долей — реально списывалось $2.50, хотя в
    # состоянии сохранялась ставка $1. Теперь считаем всё заранее.
    market_info = pt.get_market_info(token_id)
    plan = plan_order(stake_usd, entry_cents, market_info, order_kind=entry_mode)
    min_mode = get_min_size_mode()

    if plan["below_min"] and min_mode == "skip":
        log.warning(
            f"liq_strategy: {symbol} лот ${stake_usd:.2f} меньше минимума "
            f"({plan['min_shares']:g} долей = ${plan['min_cost']:.2f} при {entry_cents}¢, "
            f"тип {entry_mode}) — пропускаю"
        )
        set_setting(
            "liq_last_scan",
            f"[{symbol}] лот ${stake_usd:.2f} < минимума рынка ${plan['min_cost']:.2f} — вход пропущен",
        )
        if entry_mode == "limit":
            why = (f"Лимитный ордер ложится в стакан, поэтому Polymarket требует "
                   f"минимум {plan['min_shares']:g} долей.\n"
                   f"• переключи «🚀 Тип входа» на *market* — там минимум всего $1;\n")
        else:
            why = (f"Рыночный ордер нельзя отправить меньше чем на "
                   f"${POLY_MIN_NOTIONAL_USD:.2f}.\n")
        await _send(
            context, cid,
            f"⚠️ Пропускаю вход `{symbol}` {outcome}\n"
            f"💵 Твой лот: *${stake_usd:.2f}* ({plan['want_shares']:g} долей)\n"
            f"🚧 Минимум для входа *{entry_mode}*: *${plan['min_cost']:.2f}* "
            f"({plan['min_shares']:g} долей по {entry_cents}¢)\n\n"
            + why +
            f"• подними «💵 Первый лот» до ${plan['min_cost']:.2f} и выше;\n"
            f"• или поставь «🚧 Лот меньше минимума» = *bump*, чтобы бот "
            f"сам заходил минимально возможным размером.",
        )
        return

    shares = plan["shares"]
    if plan["below_min"]:
        log.info(
            f"liq_strategy: {symbol} лот ${stake_usd:.2f} поднят до минимума рынка "
            f"{shares:g} долей = ${plan['cost']:.2f} (режим bump)"
        )

    stake_usd_final = round(shares * entry_cents / 100.0, 4)
    demo = get_setting("demo_mode", "0") == "1"
    is_demo_flag = 1 if demo else 0

    if demo:
        order_ok = True
    elif entry_mode == "market":
        # Настоящий рыночный ордер (FOK): сумма в долларах, минимум $1,
        # ограничение в 5 долей не действует — как кнопка Market на сайте.
        res = pt.place_market_order(token_id, "BUY", stake_usd_final)
        order_ok = isinstance(res, dict) and not res.get("error")
        if order_ok:
            fill = None
            try:
                fill = pt._extract_fill(res)
            except Exception:
                fill = None
            if fill and fill.get("shares"):
                shares = round(float(fill["shares"]), 4)
                if fill.get("price"):
                    entry_cents = max(1, min(99, int(round(float(fill["price"]) * 100))))
                stake_usd_final = round(shares * entry_cents / 100.0, 4)
                log.info(f"liq_strategy: {symbol} факт исполнения — "
                         f"{shares} долей по {entry_cents}¢ (${stake_usd_final})")
        if not order_ok:
            log.warning(f"liq_strategy: market-ордер не прошёл: {res}")
            await _send(context, cid,
                        f"⚠️ Не удалось открыть сделку `{symbol}` (рыночный): `{res}`")
            return
    else:
        # Лимитный (GTC): размер в долях, уже с учётом минимума 5 долей.
        # allow_min_bump=False — молчаливая подмена размера запрещена.
        res = pt.place_order(token_id, "BUY", entry_cents / 100.0, shares,
                             allow_min_bump=False)
        order_ok = isinstance(res, dict) and not res.get("error")
        if not order_ok:
            log.warning(f"liq_strategy: ордер не прошёл: {res}")
            try:
                await _send(
                    context, cid,
                    f"⚠️ Не удалось открыть сделку `{symbol}`: `{res}`",
                    parse_mode="Markdown",
                )
            except Exception:
                pass
            return

    window_end = win_end
    start_next = win_start
    # Сохраняем реальное имя события с Polymarket, чтобы статистика потом
    # корректно показывала «Bitcoin Up or Down» / «Ethereum Up or Down» и т.д.
    raw_q = m.get("question") if isinstance(m, dict) else None
    pos = {
        "slug": slug,
        "token_id": token_id,
        "outcome": outcome,
        "stake": stake_usd_final,
        "shares": shares,
        "entry_cents": entry_cents,
        "entry_mode": entry_mode,
        "limit_price_cents": limit_price_cents,
        # Минимум лимитного ордера — нужен при продаже: мелкие позиции
        # можно закрыть только рыночным FAK.
        "min_shares": limit_min_shares(market_info),
        "window_end": window_end,
        "window_start": start_next,
        "series": series,
        "is_demo": is_demo_flag,
        "agg_snapshot": agg,
        "candle": candle,
        "symbol": symbol,
        "open_ts": now,
        "oi_snapshot": oi_data or {},
        "market_question_raw": raw_q or "",
    }
    # Доп. защита: если у этой монеты уже есть открытая позиция (гонка задач),
    # не открываем вторую.
    state.setdefault("positions", {})
    if symbol in state["positions"]:
        log.warning(f"liq_strategy: гонка — у {symbol} уже есть позиция, пропускаю")
        return
    state["positions"][symbol] = pos
    _save_state(state)

    # Заранее готовим URL для сообщения и fallback
    poly_url = _polymarket_url(slug)

    # Красивое сообщение
    try:
        total = agg["total_usd"]
        long_usd = agg["long_liq_usd"]
        short_usd = agg["short_liq_usd"]
        long_c = agg["long_count"]
        short_c = agg["short_count"]
        dom = agg["dominant"]
        by_ex = agg.get("by_exchange", {})
        threshold = float(c["liq_threshold_usd"])

        power_bar = make_power_bar(total, threshold)
        ratio = total / threshold if threshold else 0
        if ratio >= 5:
            strength = "🔴🔴🔴 МОЩНЫЙ КАСКАД"
        elif ratio >= 2:
            strength = "🔴🔴 Сильный каскад"
        else:
            strength = "🟡 Каскад"

        if dom == "SHORT":
            wiped = "🟢 Шорты"
        else:
            wiped = "🔴 Лонги"
        # Описание зависит от того, как выбрана сторона
        candle_src_raw = (agg.get("_candle_src") or "—") if isinstance(agg, dict) else "—"
        # Экранируем _ в candle_src для Markdown (gateio_spot -> gateio\_spot)
        candle_src = candle_src_raw.replace("_", "\\_")
        if candle == "DOWN" and outcome == "UP":
            dom_desc = f"свеча вниз ({candle_src}) → ставим UP (контр-трейд по спот-свече)"
            direction = f"📈 LONG (контр-трейд по спот-свече) → UP"
        elif candle == "UP" and outcome == "DOWN":
            dom_desc = f"свеча вверх ({candle_src}) → ставим DOWN (контр-трейд по спот-свече)"
            direction = f"📉 SHORT (контр-трейд по спот-свече) → DOWN"
        else:
            dom_desc = f"свеча {candle} ({candle_src}), ликвидации {dom} → fallback"
            direction = f"📊 {outcome} (fallback)"

        ex_lines = []
        exchange_parts = []
        # Сортируем биржи по объёму каскада (от большего к меньшему)
        ex_sorted = sorted(
            by_ex.items(),
            key=lambda kv: kv[1].get("long_usd", 0) + kv[1].get("short_usd", 0),
            reverse=True
        )
        # Явный статус Binance: ✅ если поймал ликвидации в окне, ⚠️ если пусто
        # (либо ошибка источника, либо действительно тихо). Сразу видно,
        # работает ли Binance-источник или нет.
        binance_ex = by_ex.get("Binance", {}) or {}
        binance_usd = float(binance_ex.get("long_usd", 0) or 0) + float(binance_ex.get("short_usd", 0) or 0)
        binance_count = int(binance_ex.get("long_count", 0) or 0) + int(binance_ex.get("short_count", 0) or 0)
        # Проверяем, является ли Binance доминирующей биржей (самой крупной в каскаде)
        max_ex_usd = 0
        if by_ex:
            max_ex_usd = max(
                (float(v.get("long_usd", 0)) + float(v.get("short_usd", 0)) for v in by_ex.values()),
                default=0,
            )
        binance_is_dominant = by_ex and binance_usd > 0 and binance_usd == max_ex_usd
        if binance_usd > 0:
            tail = " (DOMINANT)" if binance_is_dominant else ""
            binance_status = f"✅ поймал *{binance_count}* ликв. на *{_fmt_usd_compact(binance_usd)}*{tail}"
        else:
            binance_status = "⚠️ *0* ликв. в окне (тихо или источник недоступен)"
        for ex_name, ex in ex_sorted:
            l_usd = ex.get("long_usd", 0)
            s_usd = ex.get("short_usd", 0)
            l_cnt = ex.get("long_count", 0)
            s_cnt = ex.get("short_count", 0)
            ex_lines.append(f"  • {_exchange_badge(ex_name)}: ${l_usd+s_usd:,.0f} (🔴L ${l_usd:,.0f}×{l_cnt} | 🟢S ${s_usd:,.0f}×{s_cnt})")
            exchange_parts.append(f"{_exchange_short(ex_name)} ${_fmt_usd_compact(l_usd+s_usd)}")
        # Если какая-то из 4 бирж вообще не дала данных — покажем её явно
        known_exchanges = ["Binance", "Bybit", "OKX", "Gate.io"]
        missing = [e for e in known_exchanges if e not in by_ex]
        for ex_name in missing:
            ex_lines.append(f"  • {_exchange_badge(ex_name)}: $0 (тихо в этом окне)")
            exchange_parts.append(f"{_exchange_short(ex_name)} —")
        ex_text = "\n".join(ex_lines) if ex_lines else "  • нет данных"
        exchange_block = " · ".join(exchange_parts) if exchange_parts else "нет данных"

        oi_data = oi_data or {}
        oi_total = oi_data.get("total", 0)
        oi_parts = []
        for ex_name in ["Binance", "Gate.io", "Bybit", "OKX"]:
            val = oi_data.get(ex_name, 0)
            if val > 0:
                short_name = "Gate" if ex_name == "Gate.io" else ex_name
                oi_parts.append(f"{short_name} ${val/1_000_000:.0f}M")
        if oi_total > 0:
            oi_parts.append(f"Σ ${oi_total/1_000_000:.0f}M")
        oi_block = " · ".join(oi_parts) if oi_parts else "-"

        impact_block = ""
        impact_pct = 0
        if oi_total > 0:
            impact_pct = (total / oi_total) * 100
            if impact_pct >= 1.0:
                impact_label = "🔴🔴🔴 КРИТИЧЕСКИЙ"
            elif impact_pct >= 0.5:
                impact_label = "🔴🔴 Очень сильный"
            elif impact_pct >= 0.1:
                impact_label = "🔴 Сильный"
            elif impact_pct >= 0.05:
                impact_label = "🟡 Заметный"
            else:
                impact_label = "⚪ Слабый"
            impact_block = f"💪 Импакт: *{impact_pct:.3f}%* от OI ({_fmt_usd_compact(oi_total)}) — {impact_label}"

        potential_profit = round(stake_usd * (100 - entry_cents) / entry_cents, 2)
        potential_roi_pct = round((100 - entry_cents) / entry_cents * 100, 1)
        next_window_start = datetime.fromtimestamp(start_next, tz=timezone.utc).strftime("%H:%M:%S UTC")
        next_window_end = datetime.fromtimestamp(window_end, tz=timezone.utc).strftime("%H:%M:%S UTC")
        mode_emoji = "🎮 ДЕМО" if is_demo_flag else "💰 РЕАЛ"
        entry_emoji = "🚀" if entry_mode == "market" else "📋"
        entry_str = f"{entry_emoji} {('рыночный' if entry_mode == 'market' else 'лимитный')}"

        # === Топ-3 крупнейших ликвидации в окне ===
        window_sec = int(float(c["liq_window_sec"]))
        cutoff_ts = now - window_sec
        recent_for_sym = [
            e for e in _events_buffer.get(symbol, [])
            if e.get("time", 0) >= cutoff_ts
        ]
        top3 = sorted(recent_for_sym, key=lambda e: e.get("usd_value", 0), reverse=True)[:3]
        top3_lines = []
        for i, ev in enumerate(top3, 1):
            ex = ev.get("exchange", "?")
            direction_ = ev.get("direction", "?")
            usd = ev.get("usd_value", 0)
            price = ev.get("price", 0)
            ev_ts = ev.get("time", 0)
            ago = _time_ago(ev_ts)
            # Полный таймштамп в UTC — чтобы можно было сопоставить с графиком
            ts_str = datetime.fromtimestamp(float(ev_ts or now), tz=timezone.utc).strftime("%H:%M:%S UTC")
            dir_emoji = "🔴L" if direction_ == "LONG" else "🟢S"
            ex_badge = _exchange_badge(ex)
            top3_lines.append(
                f"  {i}. {dir_emoji} {ex_badge} → *{_fmt_usd_compact(usd)}* @ ${price:,.1f} | {ts_str} ({ago})"
            )
        top3_block = "\n".join(top3_lines) if top3_lines else "  (нет крупных в окне)"

        # === Возраст каскада ===
        if recent_for_sym:
            ts_min = min(e.get("time", 0) for e in recent_for_sym)
            ts_max = max(e.get("time", 0) for e in recent_for_sym)
            age_oldest = _time_ago(ts_min)
            age_youngest = _time_ago(ts_max)
            n_events = len(recent_for_sym)
            cascade_age_block = (
                f"⏱ *Возраст каскада*: {age_youngest} (свежее) — {age_oldest} (старше)\n"
                f"📊 *Событий в окне*: {n_events} (буфер: {len(_events_buffer.get(symbol,[]))}/600с)\n"
            )
        else:
            cascade_age_block = ""

        # === Текущая цена Polymarket (price_yes / price_no) ===
        cur_price_yes = m.get("price_yes") or 0
        cur_price_no = m.get("price_no") or 0
        price_block = (
            f"💹 *Polymarket сейчас*: ✅ YES *{cur_price_yes}¢* | ❌ NO *{cur_price_no}¢*\n"
        )

        # === Соотношение LONG/SHORT в % ===
        if total > 0:
            long_pct = long_usd / total * 100
            short_pct = short_usd / total * 100
        else:
            long_pct = short_pct = 0

        # === Какая биржа доминирует в каскаде (для объяснения) ===
        if by_ex:
            top_ex_name, top_ex = ex_sorted[0]
            top_ex_usd = top_ex.get("long_usd", 0) + top_ex.get("short_usd", 0)
            top_ex_pct = (top_ex_usd / total * 100) if total else 0
            dominant_ex_str = f"{_exchange_badge(top_ex_name)} ({_fmt_usd_compact(top_ex_usd)}, {top_ex_pct:.0f}%)"
        else:
            dominant_ex_str = "—"
            top_ex_name = ""

        # === Подробное объяснение входа ===
        # Собираем человеческое объяснение что произошло и почему ставим именно
        # в этот исход. Это самое информативное место — тут бот показывает
        # логическую цепочку: событие → реакция → наш выбор.
        explanation_lines = []
        # Что произошло (факт)
        if dom == "LONG":
            what_happened = (
                f"Выбиты *{_fmt_usd_compact(long_usd)}* лонгов на {long_c} ордерах "
                f"({long_pct:.0f}% каскада)"
            )
        elif dom == "SHORT":
            what_happened = (
                f"Выбиты *{_fmt_usd_compact(short_usd)}* шортов на {short_c} ордерах "
                f"({short_pct:.0f}% каскада)"
            )
        else:
            what_happened = f"LONG/SHORT примерно равны (нейтрально)"
        explanation_lines.append(f"📌 Что: {what_happened}")
        if top_ex_name:
            explanation_lines.append(
                f"📌 Где: доминирует {dominant_ex_str}"
            )
        explanation_lines.append(f"📌 Когда: {_cascade_age_inline(recent_for_sym, now)}")
        # Реакция цены
        if candle == "DOWN":
            reaction = f"свеча закрылась вниз ({candle_src}) — продавцы дожали"
            logic = f"ждём отскок вверх → ставим *UP*"
        elif candle == "UP":
            reaction = f"свеча закрылась вверх ({candle_src}) — покупатели дожали"
            logic = f"ждём откат вниз → ставим *DOWN*"
        else:
            reaction = f"свеча нейтральна ({candle_src}, doji)"
            logic = f"fallback: контр-трейд по доминирующей ликвидации ({dom}) → ставим *{outcome}*"
        explanation_lines.append(f"📌 Реакция цены: {reaction}")
        explanation_lines.append(f"📌 Логика бота: {logic}")
        # Ожидаемый сценарий и риски
        if candle == "DOWN" and outcome == "UP":
            scenario = "Если каскад продолжится — TP {tp}¢ (закроем досрочно). Иначе — страховка по рынку за {sec}с до конца окна."
        elif candle == "UP" and outcome == "DOWN":
            scenario = "Если каскад продолжится — TP {tp}¢ (закроем досрочно). Иначе — страховка по рынку за {sec}с до конца окна."
        else:
            scenario = "Контр-трейд по ликвидациям. TP {tp}¢. Страховка {sec}с до конца окна."
        new_order_time = int(float(c.get("liq_new_order_time", "3")))
        try:
            tp_for_msg = int(float(c.get("liq_tp_cents", "90")))
        except Exception:
            tp_for_msg = 90
        explanation_lines.append(
            "📌 План выхода: " + scenario.format(tp=tp_for_msg, sec=new_order_time)
        )
        explanation_block = "\n".join(explanation_lines)

        # === Сборка итогового сообщения ===
        msg = (
            f"💥 *LIQUIDATION CASCADE* 🟡Binance 🟠Bybit 🔵OKX 🟢Gate.io {mode_emoji}\n\n"
            f"🪙 *Монета:* `{asset_display_name(symbol)}` (`{symbol}`)\n"
            f"📡 *Сигнал:* *{direction}*\n"
            f"💡 *Логика:* {dom_desc}\n"
            f"🔗 *График:* [Polymarket]({poly_url})\n\n"
            f"━━━━ 🧠 *ПОЧЕМУ ЭТОТ ВХОД* ━━━━\n"
            f"{explanation_block}\n\n"
            f"━━━━ 📊 *КАСКАД* ━━━━\n"
            f"Сила: *{strength}*\n"
            f"{power_bar}\n"
            f"💵 *Итого ликвидаций:* `${total:,.0f}` за {window_sec}с "
            f"(порог ${threshold:,.0f}, ×{ratio:.1f})\n"
            f"🔴 Лонги выбиты: `${long_usd:,.0f}` ({long_c} ордеров, {long_pct:.0f}%)\n"
            f"🟢 Шорты выбиты: `${short_usd:,.0f}` ({short_c} ордеров, {short_pct:.0f}%)\n"
            f"🏦 *По биржам:* {exchange_block}\n"
            f"📡 *Binance:* {binance_status}\n"
            f"{ex_text}\n"
            f"{cascade_age_block}"
            f"\n"
            f"🏆 *Топ-3 крупнейших ликвидации :*\n"
            f"{top3_block}\n"
            f"\n"
            f"━━━━ 📈 *POLYMARKET* ━━━━\n"
            f"📊 *Slug:* `{slug}`\n"
            f"🕐 *Окно:* {next_window_start} → {next_window_end} ({tf})\n"
            + (f"✅ {md_escape(confirm_note)}\n" if confirm_note else "")
            + f"{price_block}"
            f"🎯 *Вход:* {entry_str} @ *{entry_cents}¢* → ставим *{outcome}*\n"
            f"💵 *Ставка:* `${stake_usd_final}` ({shares} shares) | "
            f"Потенциал *+${potential_profit}* (+{potential_roi_pct}%) при 100¢\n"
            f"📈 *Серия мартингейла:* {series}/{c['liq_max_series']}\n"
        )
        if oi_block and oi_block != "-":
            msg += f"\n━━━━ 📦 *OI* ━━━━\n"
            msg += f"📦 *OI по биржам:* {oi_block}\n"
            if impact_block:
                msg += f"{impact_block}\n"

        msg += f"\n💡 Свеча с `{candle_src}` (gateio\_spot = та же цена, что на графике Polymarket)"

        # === Inline-кнопка со ссылкой на Polymarket ===
        reply_markup = None
        try:
            # Подгружаем telegram-типы лениво, чтобы не падать импортом
            # в средах без python-telegram-bot (тесты).
            from telegram import InlineKeyboardButton as _Btn, InlineKeyboardMarkup as _KB
            reply_markup = _KB([[
                _Btn("📊 Открыть на Polymarket", url=poly_url)
            ]])
        except Exception:
            reply_markup = None

        await _send(
            context, cid, msg[:4000], parse_mode="Markdown",
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.warning(f"enter trade msg err {e}", exc_info=True)
        try:
            total_agg = agg.get('total_usd', 0)
            dom_agg = agg.get('dominant', '?')
            by_ex_agg = agg.get('by_exchange', {})
            ex_lines_fb = []
            for ex_name in ["Binance", "Bybit", "OKX", "Gate.io"]:
                if ex_name in by_ex_agg:
                    ex_d = by_ex_agg[ex_name]
                    ex_sum = float(ex_d.get('long_usd',0) or 0) + float(ex_d.get('short_usd',0) or 0)
                    if ex_sum > 0:
                        ex_lines_fb.append(f"{ex_name} ${ex_sum:,.0f}")
            ex_str_fb = " | ".join(ex_lines_fb) if ex_lines_fb else ""
            msg_fb = (
                f"🚨 *Сигнал* `{symbol}` → *{outcome}*\n"
                f"💥 Каскад: `${total_agg:,.0f}` ({dom_agg})\n"
                f"💵 Ставка: *{stake_usd_final}$* @ {entry_cents}¢ ({entry_mode}) серия {series}\n"
            )
            if ex_str_fb:
                msg_fb += f"📡 Биржи: {ex_str_fb}\n"
            msg_fb += f"🔗 [Polymarket]({poly_url})"
            await _send(
                context, cid, msg_fb[:4000],
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        except Exception:
            try:
                await _send(
                    context, cid,
                    f"🚨 Сигнал {symbol} {agg.get('dominant','?')} ${agg.get('total_usd',0):,.0f} → {outcome} "
                    f"{stake_usd_final}$ @{entry_cents}¢ ({entry_mode}) серия {series}",
                )
            except Exception:
                pass


async def _check_open_position(context, cid, session, c, state, symbol, pos):
    """Ведение одной открытой позиции.

    Логика выхода (по ТЗ):

    1. TP. Как только цена нашего исхода дошла до liq_tp_cents —
       выходим: sell-LIMIT ровно на TP (если позиция ≥ $5) или
       sell-MARKET (если меньше — лимитку Polymarket не примет).

    2. За liq_new_order_time секунд до конца окна смотрим СОСТОЯНИЕ
       ТЕКУЩЕЙ СВЕЧИ ЭТОГО ОКНА — выше она старта рынка или ниже
       (open свечи 5m = цена на старте окна Polymarket):

       • свеча идёт В НАШУ сторону, но TP не достигнут →
         НИЧЕГО НЕ ДЕЛАЕМ. Досрочно не закрываемся, ждём полного
         расчёта рынка. Раньше бот именно здесь и сливал: продавал
         по 40-48¢ выигрышную позицию, которая через минуту
         рассчиталась бы по 100¢.

       • свеча идёт ПРОТИВ нас → шаг проигран. Спасаем остаток
         (продаём по рынку, если получится) и сразу открываем
         СЛЕДУЮЩЕЕ окно в ТУ ЖЕ сторону с мартингейлом.

    3. После окончания окна (для удержанных позиций) ждём расчёта:
       итог берём по закрытой свече этого окна (тот же спот Gate.io,
       по которому считает Polymarket) с перепроверкой по цене рынка.
       Выиграли — серия сбрасывается. Проиграли — следующий цикл
       мартингейла в ту же сторону.
    """
    import polymarket_trading as pt

    now = time.time()
    # Если в state по какой-то причине нет обязательных полей — сбрасываем
    if not pos.get("window_end"):
        log.warning(f"liq: {symbol} позиция без window_end, сбрасываю")
        positions_map = state.setdefault("positions", {})
        positions_map.pop(symbol, None)
        _save_state(state)
        return

    time_left = pos["window_end"] - now
    new_order_time = int(float(c.get("liq_new_order_time", "3")))

    # Окно уже закрылось, а позиция всё ещё у нас — значит мы сознательно
    # держали её до расчёта (свеча шла в нашу сторону). Ждём итог рынка.
    if time_left <= 0:
        await _resolve_after_window(context, cid, session, c, state, symbol, pos)
        return

    try:
        tp_cents = int(float(c.get("liq_tp_cents", "90")))
    except Exception:
        tp_cents = 90
    tp_cents = max(2, min(99, tp_cents))

    # За сколько секунд до конца окна начинаем мониторить.
    # Должно быть >= new_order_time с запасом, чтобы успеть и TP-ордер
    # поставить, и в страховку уложиться. Минимум — 60с, чтобы захватить
    # движение цены ближе к концу.
    monitor_window = max(60, new_order_time * 20, 60)
    if time_left > monitor_window:
        # Ещё рано — возвращаемся, чтобы не долбить API впустую
        return

    # Пора принимать решение. Тянем текущие цены с Polymarket.
    info = pt.get_event_markets(pos["slug"])
    if not info or not info.get("markets"):
        # Цен нет. Раньше бот в этом месте фиксировал PnL=0, то есть
        # записывал проигрыш по сделке, которая могла и выиграть.
        # Теперь просто дожидаемся окончания окна: _resolve_after_window
        # определит итог по свече, а не по отсутствию котировки.
        log.warning(f"liq: {symbol} нет цен по {pos['slug']} — жду расчёта окна")
        return

    m = info["markets"][0]
    market_question = m.get("question", pos["slug"])
    price_yes = m.get("price_yes") or 0
    price_no = m.get("price_no") or 0
    if pos["outcome"] == "UP":
        close_price = price_yes
    else:
        close_price = price_no
    if close_price <= 0:
        # Цены ещё не появились — ждём до последней секунды
        if time_left > 0.5:
            return
        close_price = 0

    entry_cents = pos["entry_cents"]
    is_demo_flag = pos.get("is_demo", 0)
    shares = pos.get("shares", pos["stake"])
    tp_hit = close_price >= tp_cents
    in_profit = close_price > entry_cents
    emergency_time = time_left <= new_order_time

    # === Сценарий 1: TP достигнут — выходим лимиткой или рынком ===
    # Если позиция достаточно крупная (shares * tp_cents/100 >= $5),
    # ставим sell-лимитку ровно на TP-цене — это самый выгодный
    # вариант: ордер исполняется по нашей цене.
    # Если позиция маленькая (< $5 на TP) — лимитку Polymarket не
    # примет (минимальный размер ордера $5). В этом случае
    # продаём по РЫНКУ (market sell) сразу, чтобы гарантированно
    # зафиксировать прибыль, а не сидеть до конца окна.
    if tp_hit and time_left > 0:
        # Сумма, которую получим при продаже по TP-цене
        sell_value_at_tp = shares * (tp_cents / 100.0)
        # Лимитку примут, если хватает и долей (минимум рынка), и суммы.
        min_shares = float(pos.get("min_shares") or POLY_MIN_ORDER_SHARES)
        can_use_limit = (shares >= min_shares
                         and sell_value_at_tp >= POLY_MIN_NOTIONAL_USD)

        if is_demo_flag:
            # В демо — sell-market имитируем, фиксируем TP
            log.info(f"liq: 🎯 {symbol} TP {tp_cents}¢ достигнут (текущая {close_price}¢), "
                     f"ДЕМО — закрываю по TP")
            await _settle_position(context, cid, c, state, symbol, pos, win=True,
                             close_price=tp_cents, price_yes=price_yes,
                             price_no=price_no, market_question=market_question,
                             early_exit=True, settle_ts=now)
            return

        if can_use_limit:
            # Позиция крупная — sell-LIMIT ровно на TP
            sell_price = max(0.01, min(0.99, tp_cents / 100.0))
            try:
                res = pt.place_order(pos["token_id"], "SELL", sell_price, shares)
                ok = isinstance(res, dict) and not res.get("error")
                if ok:
                    log.info(f"liq: 🎯 {symbol} TP {tp_cents}¢ — лимитка выставлена "
                             f"(текущая {close_price}¢, размер ${sell_value_at_tp:.2f})")
                    pos["tp_order_placed"] = 1
                    pos["tp_placed_at"] = now
                    pos["tp_price_cents"] = tp_cents
                    state["positions"][symbol] = pos
                    _save_state(state)
                    try:
                        await _send(
                            context, cid,
                            f"🎯 *TP {tp_cents}¢ достигнут!* `{symbol}` {pos['outcome']} @ {close_price}¢\n"
                            f"💰 Выставлен SELL-лимит на {tp_cents}¢ | ждём исполнения до конца окна\n",
                            parse_mode="Markdown",
                        )
                    except Exception:
                        pass
                    return
                else:
                    log.warning(f"liq: {symbol} TP-лимитка не выставилась: {res}")
                    if time_left > 0.5:
                        return
            except Exception as e:
                log.warning(f"liq: {symbol} TP-limit exception: {e}")
                if time_left > 0.5:
                    return
            # Не вышло — на следующем тике попробуем ещё раз

        else:
            # Позиция маленькая — лимитку Polymarket не возьмёт.
            # Продаём по РЫНКУ сразу, фиксируем прибыль по текущей цене.
            sell_price = max(0.01, min(0.99, max(1, min(99, close_price)) / 100.0))
            log.info(f"liq: 🎯 {symbol} TP {tp_cents}¢ — лимитка не пройдёт "
                     f"({shares:g} долей при минимуме {min_shares:g}, "
                     f"${sell_value_at_tp:.2f}), продаём РЫНКОМ @ {close_price}¢")
            try:
                ok, sell_mode, res = sell_shares(pos, shares, close_price)
                if ok:
                    log.info(f"liq: 🎯 {symbol} TP — market sell исполнен @ {close_price}¢")
                    await _settle_position(context, cid, c, state, symbol, pos, win=True,
                                     close_price=close_price, price_yes=price_yes,
                                     price_no=price_no, market_question=market_question,
                                     early_exit=True, settle_ts=now)
                    return
                else:
                    log.warning(f"liq: {symbol} TP-market не прошёл: {res}")
                    if time_left > 0.5:
                        return
            except Exception as e:
                log.warning(f"liq: {symbol} TP-market exception: {e}")
                if time_left > 0.5:
                    return

    # === Сценарий 2: подошли к концу окна — решаем по СВЕЧЕ ===
    # Ключевое отличие от старой версии: сам по себе «убыток по цене
    # Polymarket» больше НЕ повод закрываться. Смотрим, где цена монеты
    # относительно старта окна — именно это определяет расчёт рынка.
    if emergency_time:
        window_start = pos.get("window_start") or (pos["window_end"] - TF_SECONDS.get(c["liq_timeframe"], 300))
        wc = await get_window_candle(session, symbol, c["liq_timeframe"],
                                     window_start, force=True)
        # Правило рынка: close >= open → UP. Никакой зоны «дожи» тут быть
        # не должно — Polymarket ничьих не знает.
        state_now = resolve_state(wc)
        pos["last_candle_state"] = state_now or "FLAT"
        delta_pct = 0.0
        if wc and wc.get("open"):
            delta_pct = (wc["close"] - wc["open"]) / wc["open"] * 100

        # --- 2a. Свеча в нашу сторону → держим до расчёта ---
        if state_now and state_now == pos["outcome"]:
            if not pos.get("hold_notified"):
                pos["hold_notified"] = 1
                log.info(
                    f"liq: 🤝 {symbol} свеча {state_now} совпадает с нашим "
                    f"{pos['outcome']} ({delta_pct:+.3f}% от старта окна), "
                    f"TP {tp_cents}¢ не достигнут — НЕ закрываю, жду расчёта"
                )
                await _send(
                    context, cid,
                    f"🤝 *Держим до расчёта* `{symbol}` {pos['outcome']}\n"
                    f"📈 Свеча окна: *{state_now}* ({delta_pct:+.3f}% от старта) — идёт в нашу сторону\n"
                    f"💵 Цена {close_price}¢ (вход {entry_cents}¢), TP {tp_cents}¢ не достигнут\n"
                    f"⏳ Досрочно не продаём: при таком закрытии окна выплата 100¢",
                )
            pos["hold_to_resolution"] = 1
            state["positions"][symbol] = pos
            _save_state(state)
            return

        # --- 2b. Свеча против нас (или дожи) → шаг проигран ---
        # Спасаем что можно и сразу идём в следующее окно в ТУ ЖЕ сторону.
        state_txt = state_now or "FLAT (дожи)"
        log.info(
            f"liq: ❌ {symbol} свеча окна {state_txt} против нашего "
            f"{pos['outcome']} ({delta_pct:+.3f}%) — фиксирую шаг и иду в следующее окно"
        )

        salvage_price = 0
        if is_demo_flag:
            salvage_price = close_price
        elif close_price > 0:
            # Пытаемся продать остаток — это лучше, чем дать долям
            # обнулиться при расчёте. Мелкие позиции уходят рыночным FAK.
            ok_sell, sell_mode, res = sell_shares(pos, shares, close_price)
            if ok_sell:
                salvage_price = close_price
                log.info(f"liq: {symbol} остаток продан по {close_price}¢ ({sell_mode})")
            elif sell_mode == "dust":
                log.info(
                    f"liq: {symbol} остаток {shares:g} долей на "
                    f"${shares * close_price / 100:.2f} — меньше минимальной суммы "
                    f"ордера, продать нельзя"
                )

        await _settle_position(context, cid, c, state, symbol, pos, win=False,
                               close_price=salvage_price, price_yes=price_yes,
                               price_no=price_no, market_question=market_question,
                               early_exit=True, settle_ts=now,
                               reason=f"свеча окна {state_txt} против {pos['outcome']}")
        return

    # Не наш сценарий — цена между входом и TP, времени ещё достаточно.
    # Ждём следующего тика.
    return


async def _resolve_after_window(context, cid, session, c, state, symbol, pos):
    """Расчёт позиции, которую держали до конца окна.

    Итог определяем по ЗАКРЫТОЙ свече этого окна на споте Gate.io —
    это тот же источник цены, по которому Polymarket рассчитывает
    Up/Down. Дополнительно перепроверяем ценой самого рынка: после
    расчёта выигравший исход стоит ~100¢, проигравший ~0¢.
    """
    import polymarket_trading as pt

    now = time.time()
    tf = c["liq_timeframe"]
    dur = TF_SECONDS.get(tf, 300)
    window_start = pos.get("window_start") or (pos["window_end"] - dur)
    outcome = pos["outcome"]
    waited = now - pos["window_end"]

    # 1. Свеча окна — ждём, пока она отметится закрытой
    wc = await get_window_candle(session, symbol, tf, window_start, force=True)
    result = resolve_state(wc) if (wc and wc.get("closed")) else None
    source = "свеча окна"

    # 2. Перепроверка/фолбэк по цене Polymarket
    price_yes = price_no = 0
    market_question = pos.get("market_question_raw") or pos["slug"]
    try:
        info = pt.get_event_markets(pos["slug"])
        if info and info.get("markets"):
            m = info["markets"][0]
            market_question = m.get("question", market_question)
            price_yes = m.get("price_yes") or 0
            price_no = m.get("price_no") or 0
    except Exception as e:
        log.debug(f"resolve price fetch err: {e}")

    our_price = price_yes if outcome == "UP" else price_no
    market_decided = our_price >= 97 or (our_price <= 3 and (price_yes or price_no))

    if result is None and market_decided:
        result = outcome if our_price >= 97 else ("DOWN" if outcome == "UP" else "UP")
        source = "цена рынка"

    if result is None:
        # Ещё нет данных. Ждём до 3 минут, потом решаем по цене.
        if waited < 180:
            pos["awaiting_resolution"] = 1
            state["positions"][symbol] = pos
            _save_state(state)
            set_setting(
                "liq_last_scan",
                f"[{symbol}] окно закрыто, жду расчёта ({int(waited)}с)",
            )
            return
        result = outcome if our_price >= 50 else ("DOWN" if outcome == "UP" else "UP")
        source = "фолбэк по цене"

    win = (result == outcome)
    # Выиграли — доли гасятся по 100¢; проиграли — по 0¢.
    close_price = 100 if win else 0
    log.info(
        f"liq: 🏁 {symbol} окно рассчитано: {result} ({source}), "
        f"наш {outcome} → {'WIN' if win else 'LOSS'}"
    )

    await _settle_position(context, cid, c, state, symbol, pos, win=win,
                           close_price=close_price, price_yes=price_yes,
                           price_no=price_no, market_question=market_question,
                           early_exit=False, settle_ts=now,
                           reason=f"расчёт окна: {result} ({source})")


async def _settle_position(context, cid, c, state, symbol, pos, *,
                     win: bool, close_price: int, price_yes: int, price_no: int,
                     market_question: str, early_exit: bool, settle_ts: float,
                     reason: str = ""):
    """Общая логика закрытия позиции: фиксация PnL, обновление серии,
    при проигрыше — запуск следующего шага мартингейла в новом окне.

    Вызывается как из _check_open_position (досрочно), так и из
    _check_open_position_at_end (после окончания окна).
    """
    import polymarket_trading as pt

    max_series = int(float(c["liq_max_series"]))
    is_demo_flag = pos.get("is_demo", 0)
    mode_emoji = "🎮 ДЕМО" if is_demo_flag else "💰 РЕАЛ"
    current_series = int(pos.get("series", 0) or 0)
    series_map = state.setdefault("series", {})
    positions_map = state.setdefault("positions", {})
    entry_cents = pos["entry_cents"]
    stake = pos["stake"]
    outcome = pos["outcome"]
    slug = pos["slug"]
    # Берём question, который запомнили при входе (содержит «Bitcoin Up or Down»
    # или «Ethereum Up or Down» и т.д.). Если забыли записать — fallback на
    # параметр market_question, который пришёл из _check_open_position.
    raw_q = pos.get("market_question_raw") or market_question or ""
    trade_question = _format_trade_question(symbol, raw_q)

    if win:
        # Потенциальный выигрыш: shares * (100 - entry_cents) / 100
        pnl = round(stake * (100 - entry_cents) / entry_cents, 2) if entry_cents > 0 else 0
        add_trade_history(is_demo_flag, slug, trade_question, outcome, "BUY",
                          pos.get("shares", stake), entry_cents, 100, pnl)
        series_map[symbol] = 0
        positions_map.pop(symbol, None)
        _save_state(state)
        try:
            stats_after = _get_trade_stats(is_demo_flag)
            agg_snap = pos.get("agg_snapshot", {})
            tag = "⏰ ДОСРОЧНО" if early_exit else "🏁 В СРОК"
            msg = (
                f"✅ *СДЕЛКА ЗАКРЫТА В ПЛЮС!* `{symbol}` {tag} {mode_emoji}\n\n"
                f"📈 `{slug}` {market_question[:60]}\n"
                f"🎯 {outcome} закрыт по {close_price}¢ (вход {entry_cents}¢)\n"
                f"💵 {stake}$ → +{pnl}$ | Серия {current_series}→0/{max_series}\n"
                f"💥 Каскад на входе: {agg_snap.get('dominant','?')} ${agg_snap.get('total_usd',0):,.0f}\n"
                + (f"🧭 {md_escape(reason)}\n" if reason else "")
                +
                f"📊 Всего {stats_after['total']} WR {stats_after['winrate']}% PnL {stats_after['total_pnl']}$\n"
            )
            await _send(context, cid, msg[:4000], parse_mode="Markdown")
        except Exception as e:
            log.warning(f"settle win msg err: {e}")
        return

    # === Проигрыш ===
    # Если остаток удалось продать (salvage), убыток меньше полной ставки.
    shares_cnt = pos.get("shares", stake) or 0
    recovered = round(shares_cnt * (max(0, close_price) / 100.0), 2)
    pnl = round(recovered - stake, 2)
    add_trade_history(is_demo_flag, slug, trade_question, outcome, "BUY",
                      shares_cnt, entry_cents, close_price, pnl)

    next_series = current_series + 1

    if next_series > max_series:
        # Серия полностью слита — сбрасываем по этой монете
        series_map[symbol] = 0
        positions_map.pop(symbol, None)
        _save_state(state)
        try:
            stats_after = _get_trade_stats(is_demo_flag)
            c_base = float(c["liq_base_stake"])
            c_mult = float(c["liq_martingale_mult"])
            series_loss_est = round(sum([c_base * (c_mult ** i) for i in range(max_series + 1)]), 2) if c_mult != 1 else round(c_base * (max_series + 1), 2)
            tag = "⏰ ДОСРОЧНО" if early_exit else "🏁 В СРОК"
            msg = (
                f"🛑 *СЕРИЯ СЛИТА* `{symbol}` ({max_series+1} шагов) {tag} {mode_emoji}\n"
                f"📈 `{slug}` {outcome} закрыт по {close_price}¢ (вход {entry_cents}¢)\n"
                f"💵 {stake}$ → {pnl}$ | Убыток серии ~ -{series_loss_est}$\n"
                f"📊 Всего {stats_after['total']} WR {stats_after['winrate']}% PnL {stats_after['total_pnl']}$\n"
                f"Жду новый каскад по `{symbol}`"
            )
            await _send(context, cid, msg[:4000], parse_mode="Markdown")
        except Exception as e:
            log.warning(f"settle series_bust msg err: {e}")
        return

    # === Серия продолжается: ставим следующий шаг на СЛЕДУЮЩЕЕ окно ===
    # НЕ ждём нового каскада — мартингейл идёт по таймеру.
    # ВАЖНО: сохраняем outcome ДО удаления позиции, чтобы
    # планировщик знал, в какую сторону продолжать серию.
    carried_outcome = outcome
    series_map[symbol] = next_series
    positions_map.pop(symbol, None)
    _save_state(state)

    c_base = float(c["liq_base_stake"])
    c_mult = float(c["liq_martingale_mult"])
    next_stake = round(c_base * (c_mult ** next_series), 2)
    next_potential = round(next_stake * (100 - entry_cents) / entry_cents, 2)
    log.info(f"liq: ♻️ {symbol} серия {current_series}→{next_series}/{max_series}, "
             f"следующая ставка {next_stake}$ (×{c_mult}) — планирую вход")

    # Отправляем сообщение о проигрыше
    try:
        stats_after = _get_trade_stats(is_demo_flag)
        tag = "⏰ ДОСРОЧНО" if early_exit else "🏁 В СРОК"
        msg = (
            f"🔴 *ШАГ ПРОИГРАН* `{symbol}` {tag} {mode_emoji}\n"
            f"📈 `{slug}` {outcome} закрыт по {close_price}¢ (вход {entry_cents}¢)\n"
            f"💵 {stake}$ → {pnl}$ | Серия {current_series}→{next_series}/{max_series}\n"
            f"♻️ След: {next_stake}$ ×{c_mult} @ {entry_cents}¢ (потенциал +{next_potential}$)\n"
            + (f"🧭 {md_escape(reason)}\n" if reason else "")
            +
            f"📊 {stats_after['total']} WR {stats_after['winrate']}% PnL {stats_after['total_pnl']}$\n"
            f"⏳ Сейчас вхожу в следующее окно с увеличенной ставкой…\n"
        )
        await _send(context, cid, msg[:4000], parse_mode="Markdown")
    except Exception as e:
        log.warning(f"settle step_loss msg err: {e}")

    # === ПЛАНИРОВЩИК МАРТИНГЕЙЛА: сразу входим в следующее окно ===
    # НЕ дожидаемся нового сигнала — серия идёт по таймеру.
    # Окно Polymarket up/down: 5m. Сейчас (settle_ts) — момент X текущего окна.
    # Следующее окно начинается в X+5m. Но наш бот входит в ОКНО,
    # стартующее в (now // 5m) * 5m + 5m = next 5-min boundary.
    # Так как мы закрываемся в конце текущего окна, текущий
    # boundary уже истёк — следующее окно = (now // 5m + 1) * 5m.
    tf = c["liq_timeframe"]
    next_offset = pick_entry_window(tf, settle_ts)
    next_window_start, next_window_end = _window_bounds(tf, settle_ts, offset_windows=next_offset)
    log.info(f"liq: 🪟 {symbol} следующее окно {datetime.fromtimestamp(next_window_start, tz=timezone.utc).strftime('%H:%M:%S UTC')} → "
             f"{datetime.fromtimestamp(next_window_end, tz=timezone.utc).strftime('%H:%M:%S UTC')}")

    # Небольшая задержка, чтобы Polymarket успел создать slug для нового окна
    await asyncio.sleep(1.0)

    # Открываем позицию в следующем окне с увеличенной ставкой
    try:
        await _enter_martingale_step(context, cid, c, state, symbol,
                                     next_stake, next_series, tf,
                                     carried_outcome=carried_outcome)
    except Exception as e:
        log.exception(f"martingale step enter err: {e}")
        try:
            await _send(
                context, cid, f"❌ Не удалось войти в следующее окно `{symbol}`: `{e}`",
                parse_mode="Markdown",
            )
        except Exception:
            pass


async def _enter_martingale_step(context, cid, c, state, symbol, stake_usd, series, tf,
                                 carried_outcome: str | None = None):
    """Вход в следующее окно с увеличенной ставкой. Используется
    планировщиком мартингейла после досрочного закрытия.

    В отличие от _enter_trade (которая входит по сигналу каскада),
    здесь мы входим по таймеру — серия мартингейла не ждёт новых
    ликвидаций.

    carried_outcome: outcome предыдущего шага (UP/DOWN). Если None,
    берём из state (если там осталась запись). Это гарантирует, что
    следующий шаг мартингейла ставится в ту же сторону, что и
    проигранный, а не «своевольно» в UP по умолчанию.
    """
    import polymarket_trading as pt

    now = time.time()
    # Если текущее окно только началось (например, мы дождались расчёта
    # предыдущего) — заходим в него, иначе ждём следующее.
    offset = pick_entry_window(tf, now)
    start_next, window_end = _window_bounds(tf, now, offset_windows=offset)
    slug = build_updown_slug(symbol, tf, now, offset_windows=offset)
    log.info(
        f"liq: мартингейл {symbol} — окно "
        f"{datetime.fromtimestamp(start_next, tz=timezone.utc).strftime('%H:%M:%S UTC')} "
        f"(offset {offset}, осталось {int(window_end - now)}с)"
    )

    # Outcome: сначала берём явно переданный, иначе fallback
    outcome = carried_outcome or "UP"
    candle = None
    agg = {"dominant": "NEUTRAL", "total_usd": 0, "long_liq_usd": 0, "short_liq_usd": 0,
           "long_count": 0, "short_count": 0, "by_exchange": {}}

    log.info(f"liq: мартингейл {symbol} шаг {series} продолжаем в {outcome}")

    # === Выбор цены входа по режиму ===
    entry_mode = get_entry_mode()
    limit_price_cents = int(float(c["liq_entry_price_cents"]))
    if not 1 <= limit_price_cents <= 99:
        log.error("liq: invalid entry price %s", limit_price_cents)
        return

    info = pt.get_event_markets(slug)
    if not info or not info.get("markets"):
        log.warning(f"liq: рынок {slug} ещё не сгенерирован, откладываю вход на 5 сек")
        await asyncio.sleep(5)
        info = pt.get_event_markets(slug)
    if not info or not info.get("markets"):
        log.error(f"liq: {symbol} рынок {slug} так и не появился")
        try:
            await _send(
                context, cid, f"❌ Мартингейл `{symbol}` (шаг {series}): рынок `{slug}` не появился. Серия прервана.",
                parse_mode="Markdown")
        except Exception:
            pass
        # Сбрасываем серию — продолжение невозможно
        state["series"][symbol] = 0
        _save_state(state)
        return

    m = info["markets"][0]
    token_id = m["token_yes"] if outcome == "UP" else m["token_no"]

    if entry_mode == "market":
        best_cents = int(m.get("price_yes") if outcome == "UP" else m.get("price_no")) or 0
        if best_cents <= 0:
            best_cents = limit_price_cents
        entry_cents = max(1, min(99, best_cents + 1))
    else:
        entry_cents = limit_price_cents

    # Размер шага с учётом минимума рынка (см. plan_order).
    market_info = pt.get_market_info(token_id)
    plan = plan_order(stake_usd, entry_cents, market_info, order_kind=entry_mode)
    min_mode = get_min_size_mode()

    if plan["below_min"] and min_mode == "skip":
        log.warning(
            f"liq: мартингейл {symbol} шаг {series} — ставка ${stake_usd:.2f} меньше "
            f"минимума рынка ${plan['min_cost']:.2f} ({plan['min_shares']:g} долей), серия прервана"
        )
        await _send(
            context, cid,
            f"⚠️ Мартингейл `{symbol}` (шаг {series}) остановлен\n"
            f"💵 Ставка шага: *${stake_usd:.2f}*, минимум рынка: "
            f"*${plan['min_cost']:.2f}* ({plan['min_shares']:g} долей по {entry_cents}¢)\n"
            f"Подними «💵 Первый лот» или включи режим *bump* в настройках.",
        )
        state["series"][symbol] = 0
        _save_state(state)
        return

    shares = plan["shares"]
    if plan["below_min"]:
        log.info(
            f"liq: мартингейл {symbol} шаг {series} — ставка ${stake_usd:.2f} поднята "
            f"до минимума {shares:g} долей = ${plan['cost']:.2f}"
        )

    stake_usd_final = round(shares * entry_cents / 100.0, 4)
    demo = get_setting("demo_mode", "0") == "1"
    is_demo_flag = 1 if demo else 0

    if demo:
        order_ok = True
    elif entry_mode == "market":
        res = pt.place_market_order(token_id, "BUY", stake_usd_final)
        order_ok = isinstance(res, dict) and not res.get("error")
        if order_ok:
            try:
                fill = pt._extract_fill(res)
            except Exception:
                fill = None
            if fill and fill.get("shares"):
                shares = round(float(fill["shares"]), 4)
                if fill.get("price"):
                    entry_cents = max(1, min(99, int(round(float(fill["price"]) * 100))))
                stake_usd_final = round(shares * entry_cents / 100.0, 4)
        if not order_ok:
            log.warning(f"liq: мартингейл {symbol} — market-ордер не прошёл: {res}")
            await _send(context, cid,
                        f"❌ Мартингейл `{symbol}` (шаг {series}): ордер не прошёл — `{res}`")
            state["series"][symbol] = 0
            _save_state(state)
            return
    else:
        res = pt.place_order(token_id, "BUY", entry_cents / 100.0, shares,
                             allow_min_bump=False)
        order_ok = isinstance(res, dict) and not res.get("error")
        if not order_ok:
            log.warning(f"liq: мартингейл {symbol} — ордер не прошёл: {res}")
            try:
                await _send(
                    context, cid, f"❌ Мартингейл `{symbol}` (шаг {series}): ордер не прошёл — `{res}`",
                    parse_mode="Markdown")
            except Exception:
                pass
            state["series"][symbol] = 0
            _save_state(state)
            return

    raw_q = m.get("question") if isinstance(m, dict) else None
    pos = {
        "slug": slug, "token_id": token_id, "outcome": outcome,
        "stake": stake_usd_final, "shares": shares, "entry_cents": entry_cents,
        "entry_mode": entry_mode, "limit_price_cents": limit_price_cents,
        "min_shares": limit_min_shares(market_info),
        "window_end": window_end, "window_start": start_next,
        "series": series, "is_demo": is_demo_flag,
        "agg_snapshot": agg, "candle": candle, "symbol": symbol,
        "open_ts": now, "oi_snapshot": {},
        "market_question_raw": raw_q or "",
    }
    state["positions"][symbol] = pos
    _save_state(state)

    log.info(f"liq: ✅ МАРТИНГЕЙЛ {symbol} шаг {series} вход {entry_cents}¢ "
             f"ставка {stake_usd_final}$ ({shares} shares)")

    try:
        entry_emoji = "🚀" if entry_mode == "market" else "📋"
        next_window_end_str = datetime.fromtimestamp(window_end, tz=timezone.utc).strftime("%H:%M:%S UTC")
        max_series = int(float(c["liq_max_series"]))
        msg = (
            f"♻️ *МАРТИНГЕЙЛ ШАГ {series}/{max_series}* `{symbol}` "
            f"{'🎮 ДЕМО' if is_demo_flag else '💰 РЕАЛ'}\n\n"
            f"📈 `{slug}` → *{outcome}* @ {entry_cents}¢ ({entry_emoji} {entry_mode})\n"
            f"💵 Ставка: *{stake_usd_final}$* ({shares} shares)\n"
            f"🕐 Окно до: {next_window_end_str}\n"
        )
        await _send(context, cid, msg[:4000], parse_mode="Markdown")
    except Exception as e:
        log.warning(f"martingale step msg err: {e}")
