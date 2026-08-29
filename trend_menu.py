"""Инлайн-меню второй торговой системы «Движение за рынком».

Полностью отдельное от меню «Каскада ликвидаций» (liq_menu.py):
свои кнопки, свои параметры (`td_*`), свой ввод вручную. Точки входа
в боте: callback'и `strat_trend`, `td_*`, `tqp_*`, `tqm_*`, `tqv:*`.
"""

import re
from telegram import InlineKeyboardButton as Btn, InlineKeyboardMarkup as KB

import trend_strategy as ts

# (ключ настройки, подпись, варианты для быстрого выбора)
PARAMS = [
    # td_symbols — отдельный экран-чеклист, в общий список не входит
    ("td_timeframe", "⏱ Таймфрейм окна", ["5m", "15m", "1h"]),
    ("td_candle_source", "📊 Источник свечи (направление)",
     ["chainlink", "gate_spot"]),
    ("td_entry_delay_sec", "⏳ Вход через N сек после старта окна",
     ["0", "2", "5", "10", "15"]),
    ("td_entry_window_sec", "🪟 Окно входа (сек от старта)",
     ["30", "60", "120", "180"]),
    ("td_entry_cap_cents", "🚦 Макс. цена входа, ¢ (0 — выкл)",
     ["0", "55", "60", "65", "70", "75"]),
    ("td_base_stake", "💵 Первый лот, $", ["1", "2", "5", "10", "20"]),
    ("td_martingale_mult", "✖️ Мартингейл: ×N от пред. ставки",
     ["1.5", "2", "2.5", "3"]),
    ("td_max_series", "🧮 Макс. длина серии", ["3", "5", "7", "10"]),
    ("td_tp_cents", "🏁 Take-profit, ¢", ["65", "70", "75", "80", "90", "95"]),
    ("td_tp_mode", "🎯 Как закрывать тейк", ["auto", "limit", "fak"]),
    ("td_salvage_on", "🛡 Спасение в конце окна (0/1)", ["0", "1"]),
    ("td_final_check_sec", "🛡 Спасение: сек до конца окна",
     ["5", "10", "20", "30", "45"]),
    ("td_settle_grace_sec", "⌛️ Ждать расчёт рынка, сек",
     ["0", "30", "60", "120", "300"]),
    ("td_max_concurrent", "🔒 Макс. позиций одновременно", ["1", "2", "3", "5"]),
    ("td_check_interval", "🔁 Тик поиска входа, сек", ["1", "2", "3", "5"]),
    ("td_scan_interval", "👁 Тик слежения за позицией, сек", ["1", "2", "3", "5"]),
    ("td_recent_count", "🕘 Сделок в списке «Последние»",
     ["5", "10", "15", "20", "30"]),
]

# Метаданные для ручного ввода
TREND_PARAM_META = {
    "td_entry_delay_sec": {"type": "int", "min": 0, "max": 240,
                           "hint": "Целое 0..240 секунд от старта окна — раньше не входим"},
    "td_entry_window_sec": {"type": "int", "min": 5, "max": 280,
                            "hint": "Целое 5..280: последние секунды входить поздно (цена ушла)"},
    "td_entry_cap_cents": {"type": "int", "min": 0, "max": 99,
                           "hint": "0 — выкл. Цена нашего исхода выше кэпа — вход пропускаем"},
    "td_base_stake": {"type": "float", "min": 1.0, "max": 10000.0,
                      "hint": "Первый лот в $ (минимум рыночного FAK — $1)"},
    "td_martingale_mult": {"type": "float", "min": 1.0, "max": 10.0,
                           "hint": "Каждый проигрышный шаг умножает лот на N (например 2 → 5,10,20,40$)"},
    "td_max_series": {"type": "int", "min": 1, "max": 15,
                      "hint": "После N проигрышных шагов серия сбрасывается на базовый лот"},
    "td_tp_cents": {"type": "int", "min": 51, "max": 99,
                    "hint": "Доли продаём, когда цена нашего исхода >= TP (¢)"},
    "td_final_check_sec": {"type": "int", "min": 3, "max": 120,
                           "hint": "За N сек до конца окна сверяемся со свечой и при развороте против нас спасаем остаток"},
    "td_settle_grace_sec": {"type": "int", "min": 0, "max": 600,
                            "hint": "Сколько секунд после конца окна ждать официального расчёта рынка"},
    "td_max_concurrent": {"type": "int", "min": 1, "max": 10,
                          "hint": "Сколько позиций ЭТА стратегия держит одновременно (монеты первой стратегии не блокируются)"},
    "td_check_interval": {"type": "int", "min": 1, "max": 60,
                          "hint": "Как часто проверять точку входа, сек"},
    "td_scan_interval": {"type": "int", "min": 1, "max": 60,
                         "hint": "Как часто следить за позицией и TP, сек"},
    "td_recent_count": {"type": "int", "min": 1, "max": 50,
                        "hint": "Сколько последних сделок показывать в статистике"},
}

ENUMS = {
    "td_timeframe": ["5m", "15m", "1h"],
    "td_candle_source": ["chainlink", "gate_spot"],
    "td_tp_mode": ["auto", "limit", "fak"],
    "td_salvage_on": ["0", "1"],
}

_PRETTY = {
    "auto": "🎯 Авто: отложник, иначе FAK",
    "limit": "📋 Только отложник (GTC-лимит)",
    "fak": "⚡️ Только FAK по стакану",
    "chainlink": "🔗 Chainlink TWAP",
    "gate_spot": "🟢 Спот Gate.io",
    "1": "✅ вкл", "0": "❌ выкл",
    "5m": "5m", "15m": "15m", "1h": "1h",
}


def _escape_md(text: str) -> str:
    if not text:
        return ""
    out = str(text)
    for ch in ("\\", "_", "*", "[", "]", "(", ")", "`"):
        out = out.replace(ch, "\\" + ch)
    return out


def strat_menu_kb():
    active = ts.is_active()
    return KB([
        [Btn(f"{'🟢 Работает — нажмите, чтобы остановить' if active else '🔴 Остановлена — нажмите, чтобы запустить'}",
             callback_data="td_toggle")],
        [Btn("📊 Статус", callback_data="td_status"),
         Btn("⚙️ Настройки", callback_data="td_settings")],
        [Btn("📈 Статистика", callback_data="td_stats"),
         Btn("🔄 Сбросить серии", callback_data="td_reset")],
        [Btn("⬅️ К выбору стратегий", callback_data="strat_menu")],
    ])


def settings_kb():
    selected = ts.get_selected_symbols()
    sym_label = ", ".join(selected) if selected else "(не выбрано)"
    rows = [[Btn(f"💱 Пары: {sym_label}", callback_data="tqp_td_symbols")]]
    for key, label, _opts in PARAMS:
        val = ts.get_param(key)
        if isinstance(val, str) and val in _PRETTY:
            val = _PRETTY[val]
        rows.append([Btn(f"{label}: {val}", callback_data=f"tqp_{key}")])
    rows.append([Btn("⬅️ Назад", callback_data="strat_trend")])
    return KB(rows)


def pairs_kb():
    import liq_strategy as _ls  # список доступных монет общий на бота
    selected = set(ts.get_selected_symbols())
    rows = []
    for sym in _ls.AVAILABLE_SYMBOLS:
        mark = "✅" if sym in selected else "❌"
        rows.append([Btn(f"{mark} {sym}", callback_data=f"td_tog_pair:{sym}")])
    rows.append([Btn("✅ Выбрать все", callback_data="td_pairs_all"),
                 Btn("⛔ Снять все", callback_data="td_pairs_none")])
    rows.append([Btn("⬅️ Назад", callback_data="td_settings")])
    return rows


def pairs_view_text() -> str:
    selected = ts.get_selected_symbols()
    if not selected:
        return ("💱 *Пары «Движения за рынком»*\n\n"
                "Ни одной монеты не выбрано — стратегия ждать не будет.\n"
                "Нажми на монету, чтобы добавить/убрать её.\n\n"
                "ℹ️ Выбор НЕ зависит от пар стратегии ликвидаций: монеты могут "
                "пересекаться, системы торгуют независимо.")
    lines = ["💱 *Пары «Движения за рынком»* (нажми, чтобы переключить)\n"]
    lines.append(f"Активные: *{len(selected)}* — "
                 + ", ".join(f"`{s}`" for s in selected))
    lines.append("")
    lines.append("⚠️ Не путать с парами «Каскада ликвидаций» — это отдельный список.")
    return "\n".join(lines)


def param_index(key: str) -> int:
    for i, (k, _, _) in enumerate(PARAMS):
        if k == key:
            return i
    return -1


def param_value_kb(key: str):
    label, options = None, []
    pidx = param_index(key)
    for k, lb, opts in PARAMS:
        if k == key:
            label, options = lb, opts
            break
    cur = ts.get_param(key)
    row = []
    for i, o in enumerate(options):
        pretty = _PRETTY.get(str(o), str(o))
        if str(o) == str(cur):
            pretty = f"»{pretty}«"
        row.append(Btn(pretty, callback_data=f"tqv:{pidx}:{i}"))
    rows = [row[i:i + 4] for i in range(0, len(row), 4)]
    if TREND_PARAM_META.get(key):
        rows.append([Btn("✏️ Ввести вручную", callback_data=f"tqm_{key}")])
    rows.append([Btn("⬅️ Назад", callback_data="td_settings")])
    return rows, label


def get_manual_prompt(key: str) -> str:
    meta = TREND_PARAM_META.get(key, {})
    label = next((lb for k, lb, _ in PARAMS if k == key), key)
    cur = ts.get_param(key)
    hint = meta.get("hint", "")
    return (f"✏️ *Ручной ввод — {label}*\n\n"
            f"Текущее: `{_escape_md(cur)}`\n"
            f"{_escape_md(hint)}\n\n"
            f"Отправь новое значение сообщением.\n"
            f"Для отмены — Назад.")


def validate_manual_input(key: str, raw: str):
    """Возвращает (ok, нормализованное_значение, ошибка)."""
    raw = (raw or "").strip().replace(",", ".")
    if key in ENUMS:
        allowed = ENUMS[key]
        norm = raw.lower() if raw.lower() in allowed else raw
        if norm in allowed or raw in allowed:
            return True, norm if norm in allowed else raw, ""
        return False, None, f"❌ Допустимые значения: {', '.join(allowed)}"
    meta = TREND_PARAM_META.get(key)
    if not meta:
        return False, None, "❌ Для этого параметра ручной ввод недоступен"
    t = meta.get("type")
    try:
        if t == "int":
            val = int(float(raw))
        else:
            val = float(raw)
            if int(val) == val and t != "float":
                val = int(val)
    except (TypeError, ValueError):
        return False, None, "❌ Нужно число"
    if "min" in meta and val < meta["min"]:
        return False, None, f"❌ Минимум {meta['min']}"
    if "max" in meta and val > meta["max"]:
        return False, None, f"❌ Максимум {meta['max']}"
    return True, (str(int(val)) if t == "int" else str(val)), ""
