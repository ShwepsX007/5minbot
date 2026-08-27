"""Инлайн-клавиатуры и обработчики меню настроек стратегии 'Ликвидации'."""

import re
from telegram import InlineKeyboardButton as Btn, InlineKeyboardMarkup as KB
import liq_strategy as ls

# (ключ настройки, подпись, варианты значений для быстрого выбора)
PARAMS = [
    # liq_symbols — специальный экран с чек-листом, в общий список не входит
    ("liq_timeframe", "⏱ Таймфрейм рынка", ["5m", "15m", "1h"]),
    ("liq_window_sec", "🪟 Окно агрегации, сек", ["30", "60", "120", "300"]),
    ("liq_threshold_usd", "💥 Порог каскада, $", ["50000", "150000", "300000", "500000"]),
    ("liq_check_interval", "🔁 Интервал проверки, сек", ["3", "5", "10", "15"]),
    ("liq_min_size_usd", "🔎 Мин. размер ликвидации, $", ["500", "1000", "5000", "10000"]),
    ("liq_base_stake", "💵 Первый лот, $", ["1", "2", "5", "10"]),
    ("liq_martingale_mult", "✖️ Множитель (только classic)", ["1.5", "2", "2.5", "3"]),
    ("liq_recovery_profit_pct", "♻️ Профит отыгрыша, %", ["0", "10", "15", "25", "50"]),
    ("liq_martingale_mode", "♻️ Схема мартингейла", ["recovery", "classic"]),
    ("liq_mg_entry", "👀 Шаг серии: вход", ["signal", "timer"]),
    ("liq_tail_sec", "🧪 Фильтр хвоста: последние N сек", ["0", "15", "30", "50", "120"]),
    ("liq_tail_pct", "🧪 Фильтр хвоста: порог, %", ["25", "40", "50", "75"]),
    ("liq_tail_mode", "🧪 Фильтр хвоста: не входить, если", ["above", "below"]),
    ("liq_signal_cooldown_sec", "🧊 Пауза сигналов после фильтра, сек", ["0", "60", "120", "180", "300"]),
    ("liq_mg_skip_windows", "⏭ Пропусков окон без подтверждения", ["0", "1", "2", "3"]),
    ("liq_mg_skip_lot_pct", "📊 Лот после пропусков, %", ["0", "25", "50", "100"]),
    ("liq_spread_max_cents", "🪞 Макс. спред bid-ask, ¢ (0-выкл)", ["0", "2", "3", "5"]),
    ("liq_depth_mult", "🧱 Глубина стакана × ставки (0-выкл)", ["0", "1", "2", "3"]),
    ("liq_max_entry_cents", "🚦 Макс. цена входа, ¢ (0-выкл)", ["0", "55", "60", "65", "70"]),
    ("liq_entry_mode", "🚀 Тип входа", ["market", "limit"]),
    ("liq_min_size_mode", "🚧 Лот меньше минимума рынка", ["skip", "bump"]),
    ("liq_candle_source", "📊 Источник свечи", ["chainlink", "gate_spot"]),
    ("liq_signal_candle", "🕯 Сигнальная свеча", ["current", "prev"]),
    ("liq_entry_confirm_sec", "⏳ Перепроверка свечи за N сек", ["1", "2", "3", "5"]),
    ("liq_filter_impulse", "🛡 Импульс: свечей подряд", ["0", "2", "3", "4"]),
    ("liq_filter_impulse_pct", "🛡 Импульс: движение, %", ["0.3", "0.5", "0.8", "1.2"]),
    ("liq_filter_oi_pct", "🛡 Рост OI, % (0 — выкл)", ["0", "0.3", "0.4", "0.6"]),
    ("liq_filter_cvd", "🛡 Перекос CVD (0 — выкл)", ["0", "0.45", "0.55", "0.7"]),
    ("liq_entry_price_cents", "🎯 Цена входа (лимит), ¢", ["50", "51", "55", "60"]),
    ("liq_scan_interval", "👁 Скан открытой позиции, сек", ["1", "2", "5"]),
    ("liq_new_order_time", "⏳ Фиксация исхода за N сек. до конца", ["1", "3", "5", "10"]),
    ("liq_max_series", "🧮 Макс. серия мартингейла", ["3", "5", "7", "10"]),
    ("liq_tp_cents", "🏁 Take-profit, ¢", ["80", "85", "90", "95", "99"]),
    ("liq_tp_first_cents", "🎯 Тейк 1-й ставки FAK, ¢ (0 — выкл)", ["0", "60", "70", "75", "80", "85"]),
    ("liq_tp_first_sec", "🎯 Окно 1-го тейка, сек", ["0", "15", "30", "45", "60"]),
    ("liq_recent_count", "🕘 Сделок в списке «Последние»", ["5", "10", "15", "20", "30"]),
]

# Метаданные для ручного ввода: подсказки и лимиты
LIQ_PARAM_META = {
    "liq_timeframe": {
        "type": "enum",
        "allowed": ["5m", "15m", "1h"],
        "hint": "Допустимые таймфреймы Polymarket Up/Down: 5m, 15m, 1h",
    },
    "liq_window_sec": {
        "type": "int",
        "min": 5,
        "max": 600,
        "hint": "Целое число от 5 до 600 секунд. Пример: 45",
    },
    "liq_threshold_usd": {
        "type": "int",
        "min": 1_000,
        "max": 10_000_000,
        "hint": "Порог каскада в $. От 1000 до 10000000. Пример: 75000, 200000",
    },
    "liq_check_interval": {
        "type": "int",
        "min": 1,
        "max": 60,
        "hint": "Как часто проверять ликвидации. От 1 до 60 сек. Пример: 2",
    },
    "liq_min_size_usd": {
        "type": "int",
        "min": 10,
        "max": 100_000,
        "hint": "Фильтр мелких ликвидаций в $. От 10 до 100000. Пример: 2500",
    },
    "liq_base_stake": {
        "type": "float",
        "min": 0.1,
        "max": 1000,
        "hint": "Первый лот в $. От 0.1 до 1000. Можно дробное: 1.5, 2.75",
    },
    "liq_martingale_mult": {
        "type": "float",
        "min": 1.1,
        "max": 10,
        "hint": "Множитель мартингейла. От 1.1 до 10. Пример: 2, 2.3",
    },
    "liq_recovery_profit_pct": {
        "type": "float",
        "min": 0,
        "max": 300,
        "hint": (
            "Сколько процентов ПРИБЫЛИ сверх полного отыгрыша долга приносит "
            "выигрышный шаг (схема recovery). Лот считается точно: "
            "долг × (1 + профит%) × p/(1−p). При входе 51¢ и +15% серия из "
            "5 шагов грузит депозит ~$23 на базовый $1 (вместо ~$105 при "
            "грубом ×2.2). Больше % — крупнее шаги. От 0 до 300. Пример: 15"
        ),
    },
    "liq_mg_entry": {
        "type": "enum",
        "allowed": ["signal", "timer"],
        "hint": (
            "signal — после убытка бот оценивает УБЫТОЧНУЮ свечу фильтром "
            "хвоста ликвидаций (настройки ниже): берёт все ликвидации за окно "
            "агрегации и долю последних N секунд. Если хвост прошёл — бот "
            "входит в окно СРАЗУ за убыточным, без пропусков. Если хвост "
            "заблокирован — окно после убытка пропускается, бот ждёт "
            "прохождения фильтра на следующих окнах (см. пропуски ниже).\n"
            "timer — старое поведение: вход сразу по таймеру, без проверки.\n"
            "Введи: signal или timer"
        ),
    },
    "liq_tail_sec": {
        "type": "int",
        "min": 0,
        "max": 600,
        "hint": (
            "Сколько последних секунд окна агрегации считать «хвостом». "
            "Перед самым входом бот ещё раз смотрит ВСЕ ликвидации за окно "
            "агрегации и сравнивает долю хвоста с порогом (см. следующие две "
            "настройки). 0 — фильтр выключен. Пример: 50"
        ),
    },
    "liq_tail_pct": {
        "type": "float",
        "min": 0,
        "max": 100,
        "hint": (
            "Порог доли хвоста в % от ВСЕГО объёма ликвидаций за окно "
            "агрегации. Пример: окно 300с, хвост 50с, порог 50% — если за "
            "последние 50 секунд прошло больше (или меньше — зависит от "
            "режима) 50% ликвидаций всего окна, вход отменяется."
        ),
    },
    "liq_tail_mode": {
        "type": "enum",
        "allowed": ["above", "below"],
        "hint": (
            "Когда отменять вход:\n"
            "above (выше) — если доля последних N секунд ВЫШЕ порога: "
            "каскад сконцентрирован в самом конце окна, движение может "
            "продолжаться и отката пока нет;\n"
            "below (ниже) — если доля последних N секунд НИЖЕ порога: "
            "каскад затух, «топлива» для отката уже нет.\n"
            "Введи: above (выше) или below (ниже)"
        ),
    },
    "liq_signal_cooldown_sec": {
        "type": "int",
        "min": 0,
        "max": 600,
        "hint": (
            "Сколько секунд не принимать сигналы по монете после того, как "
            "вход по ней отменил фильтр (хвост/импульс/OI/CVD). Отменённый "
            "каскад ликвидаций остаётся в окне агрегации и без паузы сразу "
            "повторно прошёл бы как «новый» сигнал. Чтобы тот же каскад "
            "гарантированно устарел, ставь паузу не меньше окна агрегации. "
            "0 — пауза выключена. Пример: 120"
        ),
    },
    "liq_mg_skip_windows": {
        "type": "int",
        "min": 0,
        "max": 5,
        "hint": (
            "Сколько окон максимум пропустить БЕЗ ВХОДА, когда фильтр хвоста "
            "блокирует продолжение серии. Убыточное окно считается первым "
            "заблокированным. После исчерпания — вход лотом «после "
            "пропусков» или остановка серии (если лот = 0%). От 0 до 5. "
            "Пример: 2"
        ),
    },
    "liq_mg_skip_lot_pct": {
        "type": "float",
        "min": 0,
        "max": 100,
        "hint": (
            "Лот после исчерпания пропусков (окон, которые не пустил фильтр "
            "хвоста), в % от расчётного. 0 — остановить серию, 100 — войти "
            "полным лотом. Пример: 50"
        ),
    },
    "liq_spread_max_cents": {
        "type": "float",
        "min": 0,
        "max": 50,
        "hint": (
            "Максимальный спред bid-ask для входа, в центах. Тонкий стакан "
            "(ночь, свежее окно) даёт плохую среднюю цену. 0 — выключить "
            "проверку. От 0 до 50. Пример: 3"
        ),
    },
    "liq_depth_mult": {
        "type": "float",
        "min": 0,
        "max": 50,
        "hint": (
            "Минимальная глубина аска в стакане, кратная ставке: глубина "
            "в пределах 3¢ от лучшей цены должна покрывать ставку "
            "умноженную на это число. 0 — выключить. Пример: 2"
        ),
    },
    "liq_max_entry_cents": {
        "type": "int",
        "min": 0,
        "max": 99,
        "hint": (
            "Максимальная цена входа в центах. Выше — payoff плохой "
            "(контр-трейд по 70¢ приносит максимум +30¢ на долю), вход "
            "пропускается. 0 — выключить. От 0 до 99. Пример: 60"
        ),
    },
    "liq_martingale_mode": {
        "type": "enum",
        "allowed": ["recovery", "classic"],
        "hint": (
            "recovery — ТОЧНЫЙ отыгрыш: лот = долг × (1+профит%) × p/(1−p). "
            "Выигрыш закрывает весь фактический долг серии и даёт заданный "
            "профит, без двойного укрупнения. Профит настраивается отдельно.\n"
            "classic — старая схема: первый лот x множитель^номер шага "
            "(1, 2, 4, 8...), потери считаются как полная ставка.\n"
            "Введи: recovery или classic"
        ),
    },
    "liq_entry_mode": {
        "type": "enum",
        "allowed": ["market", "limit"],
        "hint": (
            "market — настоящий рыночный ордер (FOK), как кнопка Market на "
            "сайте: исполняется сразу по стакану, минимум $1, ограничения "
            "в 5 долей нет.\n"
            "limit — ордер по фиксированной цене (GTC), ложится в стакан: "
            "минимум 5 долей (при 50¢ это $2.50) и может не исполниться.\n"
            "Введи: market или limit"
        ),
    },
    "liq_min_size_mode": {
        "type": "enum",
        "allowed": ["skip", "bump"],
        "hint": (
            "Минимум зависит от типа входа: рыночный (market) — $1, "
            "лимитный (limit) — 5 долей, то есть $2.50 при цене 50¢ "
            "и $1.00 при 20¢.\n"
            "skip — не входить, если твой лот меньше минимума (бот напишет, "
            "какая сумма нужна). Ставка из настроек соблюдается точно.\n"
            "bump — входить минимально возможным размером рынка (лот может "
            "оказаться больше заданного).\n"
            "Введи: skip или bump"
        ),
    },
    "liq_signal_candle": {
        "type": "enum",
        "allowed": ["current", "prev"],
        "hint": (
            "Какую свечу бот считает сигнальной.\n"
            "current — свеча, ВНУТРИ которой прошёл каскад ликвидаций "
            "(окно ещё идёт). Её же бот перепроверяет перед входом: если к "
            "закрытию она перекрасилась — откат случился без нас, вход "
            "отменяется. Рекомендуется.\n"
            "prev — последняя полностью закрытая свеча. Тогда перед входом "
            "проверяется, не ушла ли свеча ликвидаций уже в нашу сторону.\n"
            "Введи: current или prev"
        ),
    },
    "liq_candle_source": {
        "type": "enum",
        "allowed": ["chainlink", "gate_spot"],
        "hint": (
            "chainlink — TWAP Chainlink через публичный поток Polymarket "
            "(именно по нему рынок и рассчитывается: у 5m окно усреднения "
            "30 сек, у 15m/1h — 60 сек). Рекомендуется.\n"
            "gate_spot — спот Gate.io. Считается быстрее, но расходится с "
            "Polymarket: там, где на рынке явная свеча вниз, спот может "
            "показать дожи.\n"
            "Введи: chainlink или gate_spot"
        ),
    },
    "liq_entry_confirm_sec": {
        "type": "int",
        "min": 1,
        "max": 60,
        "hint": (
            "За сколько секунд до закрытия сигнальной свечи бот перепроверит "
            "её направление. Если свеча перекрасилась (откат уже случился "
            "внутри неё) — вход в следующее окно отменяется. От 1 до 60. "
            "Пример: 2"
        ),
    },
    "liq_filter_impulse": {
        "type": "int",
        "min": 0,
        "max": 10,
        "hint": (
            "Фильтр безоткатного движения. Сколько закрытых свечей подряд "
            "в одну сторону считать пампом/дампом: при таком движении "
            "контр-трейд отменяется. 0 — фильтр выключен. Пример: 3"
        ),
    },
    "liq_filter_impulse_pct": {
        "type": "float",
        "min": 0.05,
        "max": 10,
        "hint": (
            "Какое суммарное движение (в %) должна набрать серия свечей, "
            "чтобы считаться импульсом. Слишком маленькое значение будет "
            "резать обычную торговлю. Пример: 0.5"
        ),
    },
    "liq_filter_oi_pct": {
        "type": "float",
        "min": 0,
        "max": 20,
        "hint": (
            "Фильтр по открытому интересу. Наш контр-трейд рассчитан на "
            "сквиз: цену двигают ликвидации и OI падает. Если OI за 5 минут "
            "ВЫРОС больше этого значения — в рынок заходят новые деньги, "
            "отката ждать не стоит, вход отменяется. 0 — выключено. "
            "Пример: 0.4"
        ),
    },
    "liq_filter_cvd": {
        "type": "float",
        "min": 0,
        "max": 1,
        "hint": (
            "Фильтр по потоку ордеров (CVD с Binance). Доля перекоса "
            "агрессивных покупок/продаж от 0 до 1: если поток идёт против "
            "нашего входа сильнее этого значения — вход отменяется. "
            "Исключение: дивергенция цены и CVD (абсорбция) фильтр "
            "пропускает. 0 — выключено. Пример: 0.55"
        ),
    },
    "liq_entry_price_cents": {
        "type": "int",
        "min": 1,
        "max": 99,
        "hint": "Цена входа в центах. От 1 до 99. Пример: 51",
    },
    "liq_scan_interval": {
        "type": "int",
        "min": 1,
        "max": 60,
        "hint": "Скан открытой позиции. От 1 до 60 сек. Пример: 1",
    },
    "liq_new_order_time": {
        "type": "int",
        "min": 0,
        "max": 60,
        "hint": "Фиксация исхода за N сек до конца рынка. От 0 до 60. Пример: 3",
    },
    "liq_max_series": {
        "type": "int",
        "min": 1,
        "max": 20,
        "hint": "Макс. серия мартингейла. От 1 до 20. Пример: 5",
    },
    "liq_tp_cents": {
        "type": "int",
        "min": 2,
        "max": 99,
        "hint": (
            "Take-profit в центах. Когда цена нашего исхода достигает этого "
            "значения, бот продаёт по лимитке на TP. Если цена так и не дошла — "
            "за liq_new_order_time сек. до конца окна закроет по рынку. "
            "Пример: 90 (фиксируем почти всю прибыль)."
        ),
    },
    "liq_tp_first_cents": {
        "type": "int",
        "min": 0,
        "max": 99,
        "hint": (
            "Тейк по ПЕРВОЙ ставке серии (только шаг 1). Первые "
            "liq_tp_first_sec секунд окна бот пытается закрыть позицию "
            "FAK-ордером, как только цена нашего исхода доходит до этого "
            "уровня: откат после каскада часто приходит в первые секунды, "
            "а потом цену может переметнуть — лучше забрать какой есть "
            "коэффициент. Закрыл — цикл завершён, не успел — обычный режим. "
            "0 — выключено. Пример: 75"
        ),
    },
    "liq_tp_first_sec": {
        "type": "int",
        "min": 0,
        "max": 240,
        "hint": (
            "Сколько секунд с начала окна бот пытается закрыть ПЕРВУЮ ставку "
            "по уровню liq_tp_first_cents (FAK-ордером). Если за это время "
            "не закрыл — дальше обычный режим. Пример: 30"
        ),
    },
    "liq_recent_count": {
        "type": "int",
        "min": 1,
        "max": 50,
        "hint": "Сколько последних сделок показывать в блоке статистики. От 1 до 50. Пример: 20",
    },
}


def _normalize_symbol_input(raw: str) -> str | None:
    """Превращает свободный ввод в XXX_USDT, возвращает None если не похоже на символ."""
    if not raw:
        return None
    t = raw.strip().upper()
    t = t.replace(" ", "").replace("/", "_").replace("-", "_").replace("\\", "_")
    # Уже формат XXX_USDT
    if re.fullmatch(r"[A-Z0-9]{2,30}_USDT", t):
        return t
    # Формат XXXUSDT -> XXX_USDT
    m = re.fullmatch(r"([A-Z0-9]{2,30})USDT", t)
    if m:
        return f"{m.group(1)}_USDT"
    # Короткий тикер BTC -> BTC_USDT
    if re.fullmatch(r"[A-Z0-9]{2,20}", t):
        return f"{t}_USDT"
    return None


def validate_manual_input(key: str, raw_text: str):
    """
    Валидирует ручной ввод.
    Возвращает (ok: bool, normalized_value_str: str, error_or_empty: str)
    normalized_value_str — строка для сохранения в settings (через set_setting(str(value)))
    """
    text = (raw_text or "").strip()
    if not text:
        return False, "", "Пустой ввод."

    meta = LIQ_PARAM_META.get(key, {})

    if key == "liq_timeframe":
        low = text.lower().strip()
        allowed = meta.get("allowed", ["5m", "15m", "1h"])
        if low in allowed:
            return True, low, ""
        # также принять 5M -> 5m
        if low.upper() in [a.upper() for a in allowed]:
            return True, low.lower(), ""
        return False, "", f"❌ Допустимо только: {', '.join(allowed)}"

    if key == "liq_martingale_mode":
        low = text.lower().strip()
        aliases = {
            "recovery": "recovery", "рекавери": "recovery", "долг": "recovery",
            "отыгрыш": "recovery", "r": "recovery", "новый": "recovery",
            "classic": "classic", "классический": "classic", "классика": "classic",
            "старый": "classic", "c": "classic",
        }
        if low in aliases:
            return True, aliases[low], ""
        return False, "", "❌ Введи: recovery (от долга серии) или classic (старая схема)"

    if key == "liq_signal_candle":
        low = text.lower().strip()
        aliases = {
            "current": "current", "текущая": "current", "ликвидаций": "current",
            "тек": "current", "c": "current",
            "prev": "prev", "предыдущая": "prev", "пред": "prev",
            "закрытая": "prev", "p": "prev",
        }
        if low in aliases:
            return True, aliases[low], ""
        return False, "", "❌ Введи: current (свеча ликвидаций) или prev (предыдущая закрытая)"

    if key == "liq_candle_source":
        low = text.lower().strip()
        aliases = {
            "chainlink": "chainlink", "чейнлинк": "chainlink",
            "полимаркет": "chainlink", "polymarket": "chainlink",
            "twap": "chainlink", "c": "chainlink",
            "gate_spot": "gate_spot", "gate": "gate_spot", "gateio": "gate_spot",
            "спот": "gate_spot", "gate spot": "gate_spot", "g": "gate_spot",
        }
        if low in aliases:
            return True, aliases[low], ""
        return False, "", "❌ Введи: chainlink (как у Polymarket) или gate_spot"

    if key == "liq_tail_mode":
        low = text.lower().strip()
        aliases = {
            "above": "above", "выше": "above", "высок": "above",
            "больше": "above", "превышение": "above", "a": "above", "в": "above",
            "below": "below", "ниже": "below", "низк": "below",
            "меньше": "below", "затухание": "below", "b": "below", "н": "below",
        }
        matched = None
        for k, v in aliases.items():
            if low == k or low.startswith(k):
                matched = v
                break
        if matched:
            return True, matched, ""
        return False, "", "❌ Введи: above (не входить, если хвост выше порога) или below (если ниже)"

    if key == "liq_min_size_mode":
        low = text.lower().strip()
        aliases = {
            "skip": "skip", "пропустить": "skip", "пропуск": "skip",
            "s": "skip", "нет": "skip",
            "bump": "bump", "поднять": "bump", "долить": "bump",
            "минимум": "bump", "b": "bump", "да": "bump",
        }
        if low in aliases:
            return True, aliases[low], ""
        return False, "", "❌ Введи: skip (пропускать вход) или bump (заходить минимумом рынка)"

    if key == "liq_entry_mode":
        low = text.lower().strip()
        aliases = {
            "market": "market", "маркет": "market", "рыночный": "market",
            "рынок": "market", "m": "market",
            "limit": "limit", "лимит": "limit", "лимитный": "limit",
            "отложенный": "limit", "l": "limit",
        }
        if low in aliases:
            return True, aliases[low], ""
        return False, "", "❌ Введи: market (рыночный) или limit (лимитный)"

    # Числовые типы
    t = meta.get("type", "int")
    try:
        if t == "int":
            # позволяем 2.0 -> 2 но лучше требовать int
            # поддержим запятую как разделитель
            cleaned = text.replace(",", ".").replace("$", "").replace("¢", "").strip()
            # если содержит пробелы или буквы — ошибка
            # int(float()) чтобы принять "5.0"
            if "." in cleaned:
                val = int(float(cleaned))
            else:
                val = int(cleaned)
        else:  # float
            cleaned = text.replace(",", ".").replace("$", "").replace("¢", "").strip()
            val = float(cleaned)
    except ValueError:
        return False, "", f"❌ Введи число. {meta.get('hint','')}"

    min_v = meta.get("min")
    max_v = meta.get("max")
    if min_v is not None and val < min_v:
        return False, "", f"❌ Слишком мало. Минимум: {min_v}"
    if max_v is not None and val > max_v:
        return False, "", f"❌ Слишком много. Максимум: {max_v}"

    # Доп проверки для float: не бесконечность
    if t == "float":
        if val != val or val == float("inf") or val == float("-inf"):
            return False, "", "❌ Некорректное число."
        # Нормализуем: убираем лишние нули но сохраняем как строку
        # Для целых float типа 2.0 -> "2" если ключ не требует дробности? оставим как есть
        if key == "liq_base_stake" or key == "liq_martingale_mult":
            # сохраняем как есть, но с ограничением знаков
            normalized = str(round(val, 4)).rstrip("0").rstrip(".") if "." in str(val) else str(val)
            # если round даёт 2.0 -> 2, ок
            if normalized == "":
                normalized = "0"
            return True, normalized, ""
        normalized = str(val)
        return True, normalized, ""
    else:
        return True, str(int(val)), ""


def strat_menu_kb():
    active = ls.is_active()
    return KB([
        [Btn(f"{'🟢 Работает — нажмите чтобы остановить' if active else '🔴 Остановлена — нажмите чтобы запустить'}", callback_data="liq_toggle")],
        [Btn("📊 Статус", callback_data="liq_status"), Btn("⚙️ Настройки", callback_data="liq_settings")],
        [Btn("🔄 Сбросить серии", callback_data="liq_reset")],
        [Btn("⬅️ Назад", callback_data="back_main")],
    ])


def settings_kb():
    c = ls.cfg()
    selected = ls.get_selected_symbols()
    sym_label = ", ".join(selected) if selected else "(не выбрано)"
    rows = []
    # Кнопка выбора монет — первая и самая заметная
    rows.append([Btn(f"💱 Пары: {sym_label}", callback_data="lqp_liq_symbols")])
    for key, label, _ in PARAMS:
        val = c.get(key)
        # Подменяем enum-значения на человекочитаемые
        if isinstance(val, str) and val in _PRETTY_VALUE:
            val = _PRETTY_VALUE[val]
        rows.append([Btn(f"{label}: {val}", callback_data=f"lqp_{key}")])
    rows.append([Btn("⬅️ Назад", callback_data="strat_liquidations")])
    return KB(rows)


def pairs_kb():
    """Чек-лист доступных монет. Нажатие переключает выбран/не выбран."""
    selected = set(ls.get_selected_symbols())
    rows = []
    for sym in ls.AVAILABLE_SYMBOLS:
        mark = "✅" if sym in selected else "❌"
        rows.append([Btn(f"{mark} {sym}", callback_data=f"liq_tog_pair:{sym}")])
    rows.append([Btn("✅ Выбрать все", callback_data="liq_pairs_all"),
                 Btn("⛔ Снять все", callback_data="liq_pairs_none")])
    rows.append([Btn("⬅️ Назад", callback_data="liq_settings")])
    return rows


def pairs_view_text() -> str:
    selected = ls.get_selected_symbols()
    if not selected:
        return ("💱 *Выбор пар*\n\n"
                "Сейчас не выбрано ни одной монеты. Бот ждать каскады не будет.\n"
                "Нажми на монету, чтобы добавить/убрать её.\n")
    lines = ["💱 *Выбор пар* (нажми чтобы переключить)\n"]
    lines.append(f"Активные: *{len(selected)}* — " + ", ".join(f"`{s}`" for s in selected))
    lines.append("")
    lines.append("Если у пары уже открыта позиция, по ней не открывается новая до закрытия цикла.")
    return "\n".join(lines)


def param_index(key):
    for i, (k, _, _) in enumerate(PARAMS):
        if k == key:
            return i
    return -1


# Человекочитаемые подписи для значений enums (используются в кнопках).
_PRETTY_VALUE = {
    "market": "🚀 Рыночный",
    "limit":  "📋 Лимитный",
    "recovery": "🧮 Точный отыгрыш",
    "classic": "📐 Классический",
    "signal": "🧪 Через фильтр хвоста",
    "timer": "⏱ По таймеру (без фильтра)",
    "above": "⬆️ Хвост выше порога",
    "below": "⬇️ Хвост ниже порога",
    "current": "🕯 Свеча ликвидаций",
    "prev": "⏮ Предыдущая закрытая",
    "chainlink": "🔗 Chainlink TWAP",
    "gate_spot": "🟢 Спот Gate.io",
    "skip": "⏭ Пропускать вход",
    "bump": "⬆️ Заходить минимумом",
    "5m": "5m", "15m": "15m", "1h": "1h",
}


def escape_md(text: str) -> str:
    """Экранирует спец-символы Telegram Markdown в произвольной строке.

    В подсказках и значениях настроек (например liq_tp_cents.hint содержит
    «liq_new_order_time», а liq_entry_mode.hint — «market/limit») есть
    подчёркивания, звёздочки и апострофы. Если их вставить в parse_mode=Markdown
    без экранирования, Telegram-парсер спотыкается: «Can't parse entities:
    can't find end of the entity starting at byte offset N».

    Экранируем все символы, которые в Telegram MarkdownV1 имеют значение:
    _, *, [, ], (, ), `
    """
    if not text:
        return ""
    # В MarkdownV1 экранируется обратным слэшем
    return (
        str(text)
        .replace("\\", "\\\\")
        .replace("_", "\\_")
        .replace("*", "\\*")
        .replace("[", "\\[")
        .replace("]", "\\]")
        .replace("(", "\\(")
        .replace(")", "\\)")
        .replace("`", "\\`")
    )


def param_value_kb(key):
    label, options = None, []
    pidx = param_index(key)
    for k, lb, opts in PARAMS:
        if k == key:
            label, options = lb, opts
            break
    cur = ls.get_param(key)
    # callback_data кодируется как "lqv:<индекс параметра>:<индекс значения>",
    # т.к. в значениях (например BTC_USDT) есть символ "_", и его нельзя
    # использовать как разделитель при разборе callback_data.
    row = []
    for i, o in enumerate(options):
        pretty = _PRETTY_VALUE.get(str(o), str(o))
        # сохраняем маркер выбора, но подпись — человекочитаемая
        if str(o) == str(cur):
            pretty = f"»{pretty}«"
        row.append(Btn(pretty, callback_data=f"lqv:{pidx}:{i}"))
    rows = [row[i:i + 4] for i in range(0, len(row), 4)]
    meta = LIQ_PARAM_META.get(key)
    if meta:
        rows.append([Btn("✏️ Ввести вручную", callback_data=f"lqm_{key}")])
    rows.append([Btn("⬅️ Назад", callback_data="liq_settings")])
    return rows, label


def get_manual_prompt(key: str) -> str:
    """Промпт ручного ввода для параметра.

    ВАЖНО: hint и cur экранируются через escape_md. В hint встречаются
    имена настроек (liq_new_order_time) с подчёркиваниями, которые без
    экранирования ломают парсер Telegram Markdown.
    """
    meta = LIQ_PARAM_META.get(key, {})
    label = next((lb for k, lb, _ in PARAMS if k == key), key)
    cur = ls.get_param(key)
    hint = meta.get("hint", "")
    cur_safe = escape_md(cur)
    hint_safe = escape_md(hint)
    return (
        f"✏️ *Ручной ввод — {label}*\n\n"
        f"Текущее: `{cur_safe}`\n"
        f"{hint_safe}\n\n"
        f"Отправь новое значение сообщением в чат.\n"
        f"Для отмены — нажми Назад."
    )
