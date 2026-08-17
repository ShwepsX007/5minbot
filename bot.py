import logging
import uuid
import os
import math
from datetime import datetime
from urllib.parse import urlparse

from telegram import Update, InlineKeyboardButton as Btn, InlineKeyboardMarkup as KB, ReplyKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from database import *
from utils import fetch_market, build_trend, threshold_exceeded, generate_plot
import liq_strategy as ls
import liq_menu
from config import BASE_DIR, ADMIN_CHAT_IDS

log = logging.getLogger("bot")


async def md_edit(q, text, **kwargs):
    """edit_message_text с откатом на обычный текст.

    Если в статус попала битая Markdown-разметка (например, `BTC_USDT`
    вне обратных кавычек), Telegram отвечает «Can't parse entities», и
    раньше исключение уходило наверх — кнопки стратегии переставали
    отвечать. Теперь сообщение просто уходит без форматирования.
    """
    kwargs.setdefault("parse_mode", "Markdown")
    try:
        return await q.edit_message_text(text, **kwargs)
    except BadRequest as e:
        low = str(e).lower()
        if "not modified" in low:
            return None
        if "parse entities" not in low:
            raise
        log.warning(f"Markdown битый, показываю без разметки: {e}")
        kwargs.pop("parse_mode", None)
        try:
            return await q.edit_message_text(text, **kwargs)
        except BadRequest as e2:
            log.warning(f"plain edit err: {e2}")
            return None


async def md_reply(message, text, **kwargs):
    """reply_text с тем же откатом на обычный текст."""
    kwargs.setdefault("parse_mode", "Markdown")
    try:
        return await message.reply_text(text, **kwargs)
    except BadRequest as e:
        if "parse entities" not in str(e).lower():
            raise
        log.warning(f"Markdown битый, показываю без разметки: {e}")
        kwargs.pop("parse_mode", None)
        try:
            return await message.reply_text(text, **kwargs)
        except BadRequest as e2:
            log.warning(f"plain reply err: {e2}")
            return None
user_state = {}


def is_authorized(update: Update) -> bool:
    """Only explicitly configured Telegram users may operate the trading bot."""
    user = update.effective_user
    return bool(user and user.id in ADMIN_CHAT_IDS)


async def reject_unauthorized(update: Update):
    log.warning("Rejected update from unauthorized user_id=%s", getattr(update.effective_user, "id", None))
    if update.callback_query:
        await update.callback_query.answer("Доступ запрещён", show_alert=True)
    elif update.message:
        await update.message.reply_text("⛔ Доступ запрещён.")


def us(cid):
    if cid not in user_state:
        user_state[cid] = {}
    return user_state[cid]

def back(cb):
    return [Btn("⬅️ Отмена / Назад", callback_data=cb)]

# Новая клавиатура без станций
REPLY_KB = ReplyKeyboardMarkup([
    ["💰 Торговля", "📊 Рынки"],
    ["📈 Графики", "🤖 Стратегии"],
    ["⚙️ Настройки", "🔔 Уведомления"]
], resize_keyboard=True)

async def _send_internal_error(update: Update, text="❌ Внутренняя ошибка. Смотрите логи сервера."):
    try:
        if update.callback_query and update.callback_query.message:
            return await update.callback_query.message.reply_text(text)
    except: pass
    try:
        if update.message: return await update.message.reply_text(text)
    except: pass

def mk_kb():
    return KB([
        [Btn("➕ Добавить рынок", callback_data="mk_add"), Btn("📋 Список", callback_data="mk_list")],
        [Btn("🔄 Вкл/Выкл", callback_data="mk_toggle"), Btn("✏️ Переименовать", callback_data="mk_rename")],
        [Btn("🔍 Проверить рынок", callback_data="chk_market"), Btn("🗑 Удалить", callback_data="mk_delete")],
        back("back_main")
    ])

def trade_kb():
    demo = get_setting("demo_mode", "0") == "1"
    return KB([
        [Btn("📈 Купить", callback_data="tr_buy"), Btn("📉 Продать", callback_data="tr_sell")],
        [Btn("💼 Мои позиции и ордера", callback_data="tr_orders")],
        [Btn(f"🎮 Демо-режим: {'ВКЛ ✅' if demo else 'ВЫКЛ ❌'}", callback_data="tr_toggle_demo")],
        [Btn("📊 Статистика", callback_data="tr_stats")],
        [Btn("⚙️ API Настройки", callback_data="tr_api_menu"), Btn("📝 Логи", callback_data="sys_logs")],
        back("back_main"),
    ])

def api_settings_kb():
    return KB([
        [Btn("🔑 Привязать аккаунт", callback_data="trade_add_keys"), Btn("🗑 Сбросить ключи", callback_data="trade_del_keys")],
        [Btn("🔍 Диагностика", callback_data="trade_diagnose")],
        back("tr_back")
    ])

def settings_kb():
    mth = get_setting("m_threshold", "2.0")
    mi = get_setting("m_interval", "30")

    def m(v, c): return f"»{v}«" if str(v) == str(c) else str(v)

    return KB([
        [Btn("— Интервал опроса рынков —", callback_data="noop")],
        [Btn(m(f"{v}с", f"{mi}с"), callback_data=f"smi_{v}") for v in (5, 10, 20, 30, 60)],
        [Btn("— Порог уведомлений рынков —", callback_data="noop")],
        [Btn(m(f"{v}%", f"{int(float(mth))}%"), callback_data=f"smt_{v}") for v in (1, 2, 5, 10)],
        back("back_main"),
    ])

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return await reject_unauthorized(update)
    cid = update.effective_chat.id
    # ВАЖНО: раньше это нигде не вызывалось с реальным chat_id, поэтому
    # фоновые задачи (опрос рынков, SL/TP, сигналы ликвидаций) не запускались.
    schedule_jobs(context, cid)
    await update.message.reply_text("🤖 PolyBot готов к торговле! Выберите действие 👇", reply_markup=REPLY_KB)

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return await reject_unauthorized(update)
    try:
        text = update.message.text.strip()
        cid = update.effective_chat.id
        s = us(cid)
        state = s.get("state")

        if text == "💰 Торговля":
            s["state"] = None
            s.pop("liq_edit_key", None)
            import polymarket_trading as pt
            demo = get_setting("demo_mode", "0") == "1"
            if demo:
                return await update.message.reply_text("🎮 *Торговля (ДЕМО-РЕЖИМ)*", parse_mode="Markdown", reply_markup=trade_kb())
            if not pt.is_ready():
                return await update.message.reply_text(f"⚠️ API не инициализирован!\n📍 Funder: `{pt.get_wallet_address() or 'НЕТ'}`", parse_mode="Markdown", reply_markup=trade_kb())
            return await update.message.reply_text(f"💰 *Торговля*\n📍 Кошелёк: `{pt.get_wallet_address()}`\n💵 Баланс: *{pt.get_balance()}$*", parse_mode="Markdown", reply_markup=trade_kb())

        if text == "📊 Рынки":
            s["state"] = None
            s.pop("liq_edit_key", None)
            return await update.message.reply_text("📊 *Управление рынками*", parse_mode="Markdown", reply_markup=mk_kb())

        if text == "📈 Графики":
            s["state"] = None
            s.pop("liq_edit_key", None)
            mks = [m for m in get_markets() if m.get("enabled")]
            if not mks:
                return await update.message.reply_text("Нет активных рынков для графиков.")
            kb = [[Btn(f"📊 {m['name']}", callback_data=f"chrm_{m['id']}")] for m in mks] + [[Btn("❌ Закрыть", callback_data="close_inline")]]
            return await update.message.reply_text("📈 *Выберите рынок для графика:*", parse_mode="Markdown", reply_markup=KB(kb))

        if text == "🤖 Стратегии":
            s["state"] = None
            s.pop("liq_edit_key", None)
            txt = ls.get_status_text()
            if len(txt) > 3800:
                txt = txt[:3800] + "\n\n_...обрезано_"
            return await md_reply(
                update.message, "🤖 *АЛГОТОРГОВЛЯ*\n\n" + txt,
                reply_markup=liq_menu.strat_menu_kb()
            )

        if text == "⚙️ Настройки":
            s["state"] = None
            s.pop("liq_edit_key", None)
            return await update.message.reply_text("⚙️ *Настройки*", parse_mode="Markdown", reply_markup=settings_kb())

        if text == "🔔 Уведомления":
            s["state"] = None
            s.pop("liq_edit_key", None)
            mn = get_setting("market_notifications", "1") == "1"
            return await update.message.reply_text("🔔 *Уведомления*", parse_mode="Markdown", reply_markup=KB([
                [Btn(f"📊 Уведомления рынков: {'ВКЛ✅' if mn else 'ВЫКЛ❌'}", callback_data="ntg_m")],
                back("back_main")
            ]))

        # --- Ручной ввод настроек стратегии ликвидаций ---
        if state == "wait_liq_manual":
            # Если пользователь нажал на кнопку главного меню — считаем это отменой ручного ввода
            main_btns = {"💰 Торговля", "📊 Рынки", "📈 Графики", "🤖 Стратегии", "⚙️ Настройки", "🔔 Уведомления"}
            if text in main_btns:
                s["state"] = None
                s.pop("liq_edit_key", None)
                # дальше пойдёт стандартная обработка этих кнопок (ниже)
            else:
                key = s.get("liq_edit_key")
                if not key:
                    s["state"] = None
                    return await update.message.reply_text("⚠️ Неизвестный параметр, ввод отменён.", reply_markup=liq_menu.settings_kb())
                ok, norm, err = liq_menu.validate_manual_input(key, text)
                if not ok:
                    return await update.message.reply_text(
                        f"{err}\n\n{liq_menu.LIQ_PARAM_META.get(key, {}).get('hint','')}\n\nПопробуй ещё раз.",
                        reply_markup=KB([[Btn("⬅️ Назад", callback_data=f"lqp_{key}")]])
                    )
                ls.set_param(key, norm)
                s["state"] = None
                s.pop("liq_edit_key", None)
                if key in ("liq_check_interval", "liq_scan_interval"):
                    schedule_jobs(context, cid)
                # подтверждение + показать обновлённые настройки
                await update.message.reply_text(
                    f"✅ Сохранено: `{key}` = *{norm}*",
                    parse_mode="Markdown"
                )
                return await update.message.reply_text(
                    "⚙️ *Настройки стратегии «Ликвидации»*",
                    parse_mode="Markdown",
                    reply_markup=liq_menu.settings_kb()
                )

        # ==================== ОРДЕРА (ОСТАЛОСЬ БЕЗ ИЗМЕНЕНИЙ) ====================
        if state == "wait_trade_price":
            try: price_cents = int(text.strip())
            except ValueError: return await update.message.reply_text("❌ Введите целую цену от 1 до 99¢", reply_markup=KB([back("tr_back")]))
            if not 1 <= price_cents <= 99:
                return await update.message.reply_text("❌ Цена должна быть от 1 до 99¢", reply_markup=KB([back("tr_back")]))
            s["trade_price_cents"] = price_cents
            s["trade_price"] = price_cents / 100.0
            s["state"] = "wait_trade_size"
            return await update.message.reply_text("💵 Введите количество (shares):", reply_markup=KB([back("tr_back")]))

        if state == "wait_trade_size":
            try: size = float(text.replace(",", "."))
            except ValueError: return await update.message.reply_text("❌ Введите положительное число", reply_markup=KB([back("tr_back")]))
            if not math.isfinite(size) or not 0 < size <= 1_000_000:
                return await update.message.reply_text("❌ Объём должен быть положительным числом не больше 1 000 000", reply_markup=KB([back("tr_back")]))
            s["trade_size"] = size
            s["state"] = "wait_trade_tp"
            return await update.message.reply_text("🟢 Отступ Take Profit (0 - если не нужен):", reply_markup=KB([back("tr_back")]))

        if state == "wait_trade_tp":
            try: tp = int(text)
            except ValueError: return await update.message.reply_text("❌ Введите целое число", reply_markup=KB([back("tr_back")]))
            s["trade_tp_offset"] = max(0, min(99, tp))
            s["state"] = "wait_trade_sl"
            return await update.message.reply_text("🔴 Отступ Stop Loss (0 - если не нужен):", reply_markup=KB([back("tr_back")]))

        if state == "wait_trade_sl":
            try: sl = int(text)
            except ValueError: return await update.message.reply_text("❌ Введите целое число", reply_markup=KB([back("tr_back")]))
            s["trade_sl_offset"] = max(0, min(99, sl))
            s["state"] = None

            side, price, price_cents = s.get("trade_side", "BUY"), s["trade_price"], s["trade_price_cents"]
            size, outcome, question = s["trade_size"], s.get("trade_outcome", "?"), s.get("trade_question", "?")
            demo = get_setting("demo_mode", "0") == "1"

            s["abs_tp"], s["abs_sl"] = 0, 0
            tp_off, sl_off = s["trade_tp_offset"], s["trade_sl_offset"]

            if side == "BUY":
                if tp_off > 0: s["abs_tp"] = min(99, price_cents + tp_off)
                if sl_off > 0: s["abs_sl"] = max(1, price_cents - sl_off)
            else:
                if tp_off > 0: s["abs_tp"] = max(1, price_cents - tp_off)
                if sl_off > 0: s["abs_sl"] = min(99, price_cents + sl_off)

            msg = (f"{'🎮 [ДЕМО] ' if demo else ''}{'🟢' if side == 'BUY' else '🔴'} *Подтверждение*\n\n"
                   f"📊 Рынок: {question}\n🎯 Исход: *{outcome}*\n🔘 Действие: *{'Купить' if side == 'BUY' else 'Продать'}*\n"
                   f"💲 Цена: *{price_cents}¢* | 📦 Объём: *{size}*\n💰 Итого: *{round(price * size, 2)}$*\n")
            if tp_off > 0 or sl_off > 0:
                msg += "\n⚙️ *Автозакрытие:*\n"
                if tp_off > 0: msg += f"🟢 Take Profit: при *{s['abs_tp']}¢*\n"
                if sl_off > 0: msg += f"🔴 Stop Loss: при *{s['abs_sl']}¢*\n"

            return await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=KB([[Btn("✅ Подтвердить", callback_data="tr_confirm"), Btn("❌ Отмена", callback_data="tr_back")]]))

        # ==================== ОСТАЛЬНОЙ ВВОД ====================
        if state == "wait_poly_pk":
            pk = text.lower()
            if pk.startswith("0x"): pk = pk[2:]
            if len(pk) != 64 or any(ch not in "0123456789abcdef" for ch in pk):
                return await update.message.reply_text("❌ Некорректный приватный ключ.")
            s["temp_pk"] = "0x" + pk
            try:
                await update.message.delete()
            except Exception:
                log.warning("Could not delete private-key message")
            s["state"] = "wait_poly_funder"
            return await update.message.reply_text("Отправьте *Funder адрес* (0x...).", parse_mode="Markdown")

        if state == "wait_poly_funder":
            funder = text
            pk = s.get("temp_pk")
            s["state"] = None
            import polymarket_trading as pt
            status = await update.message.reply_text("⏳ Инициализация API...")
            pt.update_env_and_config({"POLY_PRIVATE_KEY": pk, "POLY_FUNDER": funder, "POLY_SIGNATURE_TYPE": "3"})
            keys = pt.auto_generate_polymarket_keys(pk)
            if keys: pt.update_env_and_config(keys)
            if pt.init_trading(): return await status.edit_text("✅ Аккаунт подключён!", reply_markup=KB([back("tr_back")]))
            return await status.edit_text("⚠️ Ошибка авторизации. Проверьте логи.", reply_markup=KB([back("tr_back")]))

        if state == "wait_mk_url":
            try:
                slug = urlparse(text).path.split("/event/")[1].split("/")[0]
                s.update({"slug": slug, "state": "wait_mk_name"})
                return await update.message.reply_text("Имя рынка:")
            except: return await update.message.reply_text("❌ Ошибка ссылки.")

        if state == "wait_mk_name":
            add_market(text, s["slug"])
            s["state"] = None
            return await update.message.reply_text(f"✅ Рынок добавлен: {text}", reply_markup=mk_kb())

        if state == "wait_mk_rename_name":
            mid = s.get("rename_mk_id")
            if mid:
                update_market(mid, name=text)
                s["state"] = None
                return await update.message.reply_text(f"✅ Рынок переименован: {text}", reply_markup=mk_kb())

    except Exception as e:
        log.exception(f"on_text error: {e}")
        await _send_internal_error(update)

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return await reject_unauthorized(update)
    try:
        q = update.callback_query
        try: await q.answer()
        except: pass

        cid = q.message.chat_id
        d = q.data
        s = us(cid)

        if d == "noop": return
        if d == "close_inline": return await q.delete_message()
        if d == "back_main":
            s["state"] = None
            return await q.edit_message_text("📋 Главное меню. Используйте кнопки внизу 👇", parse_mode="Markdown")

        if d == "ntg_m":
            mn = get_setting("market_notifications", "1") == "1"
            set_setting("market_notifications", "0" if mn else "1")
            mn = not mn
            return await q.edit_message_text("🔔 *Уведомления*", parse_mode="Markdown", reply_markup=KB([
                [Btn(f"📊 Уведомления рынков: {'ВКЛ✅' if mn else 'ВЫКЛ❌'}", callback_data="ntg_m")],
                back("back_main")
            ]))

        # === СТРАТЕГИЯ "ЛИКВИДАЦИИ" ===
        if d == "strat_liquidations":
            s["state"] = None
            s.pop("liq_edit_key", None)
            txt = ls.get_status_text()
            # Telegram limit 4096, keep safe margin for markup
            if len(txt) > 3800:
                txt = txt[:3800] + "\n\n_...обрезано, полный статус в логах_"
            return await md_edit(q, "🤖 *АЛГОТОРГОВЛЯ*\n\n" + txt, reply_markup=liq_menu.strat_menu_kb())

        if d == "liq_toggle":
            s["state"] = None
            s.pop("liq_edit_key", None)
            ls.set_active(not ls.is_active())
            schedule_jobs(context, cid)
            txt = ls.get_status_text()
            if len(txt) > 3800:
                txt = txt[:3800] + "\n\n_...обрезано_"
            return await md_edit(q, "🤖 *АЛГОТОРГОВЛЯ*\n\n" + txt, reply_markup=liq_menu.strat_menu_kb())

        if d == "liq_status":
            s["state"] = None
            s.pop("liq_edit_key", None)
            txt = ls.get_status_text()
            if len(txt) > 3800:
                txt = txt[:3800] + "\n\n_...обрезано_"
            # Reply instead of editing the unchanged menu: Telegram rejects an
            # identical edit with HTTP 400, which made Status appear unresponsive.
            return await md_reply(
                q.message, "🤖 *АЛГОТОРГОВЛЯ*\n\n" + txt,
                reply_markup=liq_menu.strat_menu_kb()
            )

        if d == "liq_reset":
            s["state"] = None
            s.pop("liq_edit_key", None)
            ls.reset_state()
            txt = ls.get_status_text()
            if len(txt) > 3800:
                txt = txt[:3800] + "\n\n_...обрезано_"
            return await md_edit(q, "✅ Серия сброшена.\n\n" + txt, reply_markup=liq_menu.strat_menu_kb())

        if d == "liq_settings":
            s["state"] = None
            s.pop("liq_edit_key", None)
            return await q.edit_message_text("⚙️ *Настройки стратегии «Ликвидации»*", parse_mode="Markdown", reply_markup=liq_menu.settings_kb())

        if d == "lqp_liq_symbols":
            s["state"] = None
            s.pop("liq_edit_key", None)
            return await q.edit_message_text(
                liq_menu.pairs_view_text(),
                parse_mode="Markdown",
                reply_markup=KB(liq_menu.pairs_kb()),
            )

        if d.startswith("liq_tog_pair:"):
            sym = d[len("liq_tog_pair:"):].strip().upper()
            current = ls.get_selected_symbols()
            if sym in current:
                new_list = [x for x in current if x != sym]
            else:
                new_list = list(current) + [sym]
            ls.set_selected_symbols(new_list)
            # Обновим подписку Bybit WS, чтобы новые монеты начали литься
            try:
                import liq_api as _la
                _la.set_symbols(ls.get_selected_symbols())
            except Exception as e:
                log.debug(f"set_symbols err: {e}")
            return await q.edit_message_text(
                liq_menu.pairs_view_text(),
                parse_mode="Markdown",
                reply_markup=KB(liq_menu.pairs_kb()),
            )

        if d == "liq_pairs_all":
            ls.set_selected_symbols(list(ls.AVAILABLE_SYMBOLS))
            try:
                import liq_api as _la
                _la.set_symbols(ls.get_selected_symbols())
            except Exception as e:
                log.debug(f"set_symbols err: {e}")
            return await q.edit_message_text(
                liq_menu.pairs_view_text(),
                parse_mode="Markdown",
                reply_markup=KB(liq_menu.pairs_kb()),
            )

        if d == "liq_pairs_none":
            ls.set_selected_symbols([])
            return await q.edit_message_text(
                liq_menu.pairs_view_text(),
                parse_mode="Markdown",
                reply_markup=KB(liq_menu.pairs_kb()),
            )

        if d.startswith("lqp_"):
            key = d[4:]
            s["state"] = None
            s.pop("liq_edit_key", None)
            rows, label = liq_menu.param_value_kb(key)
            cur = ls.get_param(key)
            # Человекочитаемое отображение текущего значения
            cur_pretty = liq_menu._PRETTY_VALUE.get(str(cur), str(cur))
            hint = liq_menu.LIQ_PARAM_META.get(key, {}).get("hint", "")
            # Экранируем спец-символы Markdown в подсказке и значении,
            # иначе Telegram не сможет распарсить текст (например
            # "liq_new_order_time" содержит "_" и ломает парсер).
            hint_safe = liq_menu.escape_md(hint) if hint else ""
            cur_pretty_safe = liq_menu.escape_md(cur_pretty)
            if hint_safe:
                text = f"⚙️ {label} — текущее: *{cur_pretty_safe}*\n\n{hint_safe}"
            else:
                text = f"⚙️ {label} — выберите значение:"
            return await q.edit_message_text(text, parse_mode="Markdown", reply_markup=KB(rows))

        if d.startswith("lqm_"):
            key = d[4:]
            s["state"] = "wait_liq_manual"
            s["liq_edit_key"] = key
            prompt = liq_menu.get_manual_prompt(key)
            return await q.edit_message_text(
                prompt,
                parse_mode="Markdown",
                reply_markup=KB([
                    [Btn("⬅️ Назад", callback_data=f"lqp_{key}")],
                    [Btn("⬅️ К настройкам", callback_data="liq_settings")]
                ])
            )

        if d.startswith("lqv:"):
            _, pidx_s, vidx_s = d.split(":")
            pidx, vidx = int(pidx_s), int(vidx_s)
            key, _, options = liq_menu.PARAMS[pidx]
            value = options[vidx]
            ls.set_param(key, value)
            s["state"] = None
            s.pop("liq_edit_key", None)
            if key in ("liq_check_interval", "liq_scan_interval"):
                schedule_jobs(context, cid)  # переинициализировать интервалы фоновых задач
            return await q.edit_message_text("⚙️ *Настройки стратегии «Ликвидации»*", parse_mode="Markdown", reply_markup=liq_menu.settings_kb())

        # === РЫНКИ ===
        if d == "mk_add":
            s["state"] = "wait_mk_url"
            return await q.edit_message_text("📊 Отправьте ссылку на рынок Polymarket:", reply_markup=KB([back("back_main")]))

        if d == "mk_list":
            mks = get_markets()
            if not mks: return await q.edit_message_text("📋 Список рынков пуст.", reply_markup=mk_kb())
            msg = "📋 *Список рынков:*\n\n"
            for m in mks:
                status = "✅" if m.get("enabled") else "❌"
                probs = m.get("last_probs", {})
                prob_str = " | ".join([f"{k}: {v}%" for k, v in list(probs.items())[:2]]) if probs else "нет данных"
                msg += f"{status} 📊 *{m['name']}*\n🔗 `{m['slug']}`\n📈 {prob_str}\n\n"
            return await q.edit_message_text(msg[:4000], parse_mode="Markdown", reply_markup=mk_kb())

        if d == "mk_toggle":
            kb = [[Btn(f"{'✅' if m.get('enabled') else '❌'} {m['name']}", callback_data=f"mk_tog_{m['id']}")] for m in get_markets()] + [back("back_main")]
            return await q.edit_message_text("🔄 Выберите рынок:", reply_markup=KB(kb))

        if d.startswith("mk_tog_"):
            mid = int(d[7:])
            m = get_market(mid)
            if m:
                update_market(mid, enabled=0 if m.get("enabled") else 1)
                return await q.edit_message_text(f"Обновлено: {m['name']}", reply_markup=mk_kb())

        if d == "mk_rename":
            kb = [[Btn(f"📊 {m['name']}", callback_data=f"mk_ren_{m['id']}")] for m in get_markets()] + [back("back_main")]
            return await q.edit_message_text("✏️ Выберите рынок:", reply_markup=KB(kb))

        if d.startswith("mk_ren_"):
            s["state"], s["rename_mk_id"] = "wait_mk_rename_name", int(d[7:])
            return await q.edit_message_text("✏️ Введите новое имя:", reply_markup=KB([back("back_main")]))

        if d == "mk_delete":
            kb = [[Btn(f"🗑 {m['name']}", callback_data=f"mk_del_{m['id']}")] for m in get_markets()] + [back("back_main")]
            return await q.edit_message_text("🗑 Выберите рынок для удаления:", reply_markup=KB(kb))

        if d.startswith("mk_del_"):
            delete_market(int(d[7:]))
            return await q.edit_message_text("✅ Удалено.", reply_markup=mk_kb())

        if d == "chk_market":
            kb = [[Btn(f"📊 {m['name']}", callback_data=f"chk_mk_{m['id']}")] for m in get_markets()] + [back("back_main")]
            return await q.edit_message_text("📊 Выберите рынок для проверки:", reply_markup=KB(kb))

        if d.startswith("chk_mk_"):
            m = get_market(int(d[7:]))
            if not m: return
            await q.edit_message_text(f"⏳ Загружаю *{m['name']}*...", parse_mode="Markdown")
            md = fetch_market(m["slug"])
            if not md or not md.get("options"): return await q.edit_message_text(f"❌ Нет данных", reply_markup=mk_kb())
            msg = f"📊 *{md['title']}*\n\n{build_trend(m.get('last_probs') or {}, md)}"
            return await q.edit_message_text(msg[:4000], parse_mode="Markdown", reply_markup=mk_kb())

        # === ТОРГОВЛЯ (Весь блок остался неизменным, просто убраны отсылки на погоду) ===
        if d == "tr_back":
            s["state"] = None
            import polymarket_trading as pt
            demo = get_setting("demo_mode", "0") == "1"
            if demo: return await q.edit_message_text("🎮 *Торговля (ДЕМО)*", parse_mode="Markdown", reply_markup=trade_kb())
            if not pt.is_ready(): return await q.edit_message_text("⚠️ Клиент не готов!", reply_markup=trade_kb())
            return await q.edit_message_text(f"💰 *Торговля*\nБаланс: *{pt.get_balance()}$*", parse_mode="Markdown", reply_markup=trade_kb())

        if d == "tr_api_menu":
            return await q.edit_message_text("⚙️ *Настройки Polymarket API*", parse_mode="Markdown", reply_markup=api_settings_kb())

        if d == "trade_diagnose":
            import polymarket_trading as pt
            return await q.edit_message_text(f"🔍 *Диагностика*\n🔑 EOA: `{pt.get_eoa_address()}`\n🤖 Клиент: {'✅ ОК' if pt.is_ready() else '❌ Ошибка'}", parse_mode="Markdown", reply_markup=KB([[Btn("🔄 Переинициализировать", callback_data="trade_reinit")], back("tr_api_menu")]))

        if d == "trade_reinit":
            import polymarket_trading as pt
            if pt.init_trading(): return await q.edit_message_text("✅ Подключено!", reply_markup=KB([back("tr_api_menu")]))
            return await q.edit_message_text("❌ Ошибка.", reply_markup=KB([[Btn("🔑 Привязать", callback_data="trade_add_keys")], back("tr_api_menu")]))

        if d == "trade_add_keys":
            s["state"] = "wait_poly_pk"
            return await q.edit_message_text("Отправьте *Приватный Ключ*.", parse_mode="Markdown", reply_markup=KB([back("tr_api_menu")]))

        if d == "trade_del_keys":
            import polymarket_trading as pt
            pt.update_env_and_config({"POLY_PRIVATE_KEY": "", "POLY_API_KEY": "", "POLY_API_SECRET": "", "POLY_API_PASSPHRASE": "", "POLY_FUNDER": ""})
            pt._client = None
            return await q.edit_message_text("✅ Ключи сброшены.", reply_markup=KB([back("tr_api_menu")]))

        if d == "tr_toggle_demo":
            demo = get_setting("demo_mode", "0") != "1"
            set_setting("demo_mode", "1" if demo else "0")
            return await q.edit_message_text(
                "🎮 *Торговля (ДЕМО-РЕЖИМ)*" if demo else "💰 *Торговля*",
                parse_mode="Markdown", reply_markup=trade_kb()
            )

        if d in ("tr_buy", "tr_sell"):
            s["trade_side"] = "BUY" if d == "tr_buy" else "SELL"
            kb = [[Btn(f"📊 {m['name']}", callback_data=f"trm_{m['id']}")] for m in get_markets()] + [back("tr_back")]
            return await q.edit_message_text(f"{'📈 Купить' if d == 'tr_buy' else '📉 Продать'} — выберите рынок:", reply_markup=KB(kb))

        if d.startswith("trm_"):
            import polymarket_trading as pt
            mk = get_market(int(d[4:]))
            if not mk: return
            s["trade_slug"], s["trade_market_name"] = mk["slug"], mk["name"]
            info = pt.get_event_markets(mk["slug"])
            if not info or not info.get("markets"): return await q.edit_message_text("❌ Ошибка.", reply_markup=KB([back("tr_back")]))
            s["trade_event"] = info

            active = [m for m in info["markets"] if m.get("active", True)] or info["markets"]
            msg, kb = f"💰 *{info['title']}*\nВыберите исход:\n\n", []
            for i, m in enumerate(active[:20]):
                py, pn, qs = m["price_yes"], m["price_no"], m["question"][:40]
                msg += f"*{i+1}.* {qs}\n   ✅ Yes: {py}¢ | ❌ No: {pn}¢\n\n"
                kb.append([Btn(f"✅ YES {py}¢", callback_data=f"try_{i}"), Btn(f"❌ NO {pn}¢", callback_data=f"trn_{i}")])
            kb.append(back("tr_back"))
            return await q.edit_message_text(msg, parse_mode="Markdown", reply_markup=KB(kb))

        if d.startswith("try_") or d.startswith("trn_"):
            idx = int(d[4:])
            active = [m for m in s.get("trade_event", {}).get("markets", []) if m.get("active", True)] or s.get("trade_event", {}).get("markets", [])
            m = active[idx]
            is_yes = d.startswith("try_")
            s["trade_token_id"], s["trade_outcome"], s["trade_question"] = m["token_yes"] if is_yes else m["token_no"], "YES" if is_yes else "NO", m["question"]
            s["state"] = "wait_trade_price"
            return await q.edit_message_text(f"{'✅' if is_yes else '❌'} *{m['question']}* → {s['trade_outcome']}\n\n💲 Введите цену (1-99¢):", parse_mode="Markdown", reply_markup=KB([back("tr_back")]))

        if d == "tr_confirm":
            demo = get_setting("demo_mode", "0") == "1"
            await q.edit_message_text("⏳ Размещаю ордер...")
            if demo:
                success, order_id = True, f"DEMO-{uuid.uuid4().hex[:8]}"
            else:
                import polymarket_trading as pt
                res = pt.place_order(s.get("trade_token_id"), s.get("trade_side"), s.get("trade_price"), s.get("trade_size"))
                if isinstance(res, dict) and res.get("error"): return await q.edit_message_text(f"❌ Ошибка:\n`{res['error']}`", parse_mode="Markdown", reply_markup=KB([back("tr_back")]))
                success, order_id = (res.get("success", False) if isinstance(res, dict) else True), (res.get("orderID", "unknown") if isinstance(res, dict) else "unknown")

            if success:
                add_position(1 if demo else 0, s.get("trade_slug", ""), s.get("trade_token_id"), s.get("trade_side"), s.get("trade_size"), s.get("abs_sl", 0), s.get("abs_tp", 0), s.get("trade_price_cents", 0), s.get("trade_question", "?"), s.get("trade_outcome", "?"))
            return await q.edit_message_text(f"✅ *Ордер размещён!* \n🆔 ID: `{str(order_id)[:12]}...`", parse_mode="Markdown", reply_markup=KB([back("tr_back")]))

        # Мои ордера, Статистика и прочее остаются без изменений, логика Polymarket сохранена 1в1
        if d == "tr_orders":
            import polymarket_trading as pt
            demo = get_setting("demo_mode", "0") == "1"
            positions = [p for p in get_positions() if p["is_demo"] == (1 if demo else 0)]
            open_orders = [] if demo else pt.get_open_orders()
            if not positions and not open_orders: return await q.edit_message_text("📋 Нет открытых позиций.", reply_markup=KB([back("tr_back")]))

            msg, kb = f"💼 *ПОРТФЕЛЬ ({'🎮 ДЕМО' if demo else '💰 РЕАЛ'}):*\n\n", []
            s["cancel_map"] = {}
            for p in positions[:8]:
                info = pt.get_event_markets(p["slug"])
                curr_price = 0
                if info:
                    for m in info.get("markets", []):
                        if m["token_yes"] == p["token_id"]: curr_price = m["price_yes"]
                        elif m["token_no"] == p["token_id"]: curr_price = m["price_no"]
                ep = int(p["entry_price"])
                pnl = round(((curr_price - ep) if p["side"] == "BUY" else (ep - curr_price)) * p["size"] / 100, 2)
                msg += f"📦 *{p['question']}*\n🔹 {p['side']} ({p['outcome']}) | Вход: *{ep}¢* | Текущая: *{curr_price}¢*\n🔹 SL: *{p['sl']}¢* | TP: *{p['tp']}¢* | PnL: {'+' if pnl>0 else ''}{pnl}$\n\n"
                kb.append([Btn(f"🛑 Продать сейчас ({curr_price}¢)", callback_data=f"pos_close_{p['id']}_{curr_price}")])
                kb.append([Btn("📉 SL = 0", callback_data=f"pos_sl0_{p['id']}"), Btn("📈 TP = 100", callback_data=f"pos_tp100_{p['id']}")])
                kb.append([Btn("🗑 Убрать из трекера", callback_data=f"pos_forget_{p['id']}")])

            if open_orders:
                msg += "\n📦 *ЛИМИТНЫЕ ОРДЕРА:*\n"
                for i, o in enumerate(open_orders[:5]):
                    s["cancel_map"][str(i)] = o.get("id", "?")
                    msg += f"🔸 `{o.get('id', '?')[:8]}...` {o.get('side')} {o.get('original_size')} шт.\n"
                    kb.append([Btn(f"❌ Отменить ордер", callback_data=f"trc_{i}")])
            kb.append(back("tr_back"))
            return await q.edit_message_text(msg[:4000], parse_mode="Markdown", reply_markup=KB(kb))

        if d == "tr_stats":
            demo = get_setting("demo_mode", "0") == "1"
            trades = get_trade_statistics(1 if demo else 0)
            if not trades:
                return await q.edit_message_text(f"📊 Статистика пуста ({'ДЕМО' if demo else 'РЕАЛ'}).", reply_markup=KB([back("tr_back")]))
            total_pnl = round(sum(t["pnl"] for t in trades), 2)
            wins = sum(1 for t in trades if t["pnl"] > 0)
            msg = (f"📊 *Статистика ({'🎮 ДЕМО' if demo else '💰 РЕАЛ'})*\n\n"
                   f"Всего сделок: *{len(trades)}*\nВ плюс: *{wins}* | В минус: *{len(trades) - wins}*\n"
                   f"Суммарный PnL: *{'+' if total_pnl >= 0 else ''}{total_pnl}$*\n\n*Последние сделки:*\n")
            for t in trades[:10]:
                msg += f"{'🟢' if t['pnl'] >= 0 else '🔴'} {t['question'][:30]} | {t['side']} {t['outcome']} | {'+' if t['pnl']>=0 else ''}{t['pnl']}$\n"
            return await q.edit_message_text(msg[:4000], parse_mode="Markdown", reply_markup=KB([
                [Btn("🗑 Очистить статистику", callback_data="tr_stats_clear")], back("tr_back")
            ]))

        if d == "tr_stats_clear":
            clear_trade_statistics(1 if get_setting("demo_mode", "0") == "1" else 0)
            return await q.edit_message_text("✅ Статистика очищена.", reply_markup=KB([back("tr_back")]))

        if d == "sys_logs":
            log_path = os.path.join(BASE_DIR, "bot.log")
            if not os.path.exists(log_path):
                return await q.edit_message_text("📝 Файл логов пока пуст.", reply_markup=KB([back("tr_back")]))
            try:
                with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()[-40:]
                text = "".join(lines) or "пусто"
            except Exception as e:
                text = f"Ошибка чтения логов: {e}"
            return await q.edit_message_text(f"📝 *Последние строки логов:*\n```\n{text[-3500:]}\n```", parse_mode="Markdown", reply_markup=KB([back("tr_back")]))

        if d.startswith("chrm_"):
            mid = int(d[5:])
            m = get_market(mid)
            if not m: return await q.edit_message_text("❌ Рынок не найден.")
            hist = get_market_history(mid, hours=24)
            buf = generate_plot(hist, m["name"])
            if not buf:
                return await q.edit_message_text("📉 Недостаточно данных для графика (подождите, пока накопится история).", reply_markup=KB([back("close_inline")]))
            await q.message.reply_photo(photo=buf, caption=f"📈 {m['name']} — 24ч")
            return await q.delete_message()

        if d.startswith("pos_close_"):
            rest = d[len("pos_close_"):]
            pid_str, _, price_str = rest.rpartition("_")
            pid, price = int(pid_str), int(price_str)
            pos = get_position_by_id(pid)
            if not pos: return await q.edit_message_text("❌ Позиция не найдена.", reply_markup=KB([back("tr_back")]))
            success = True
            if pos["is_demo"] == 0:
                import polymarket_trading as pt
                res = pt.place_order(pos["token_id"], "SELL" if pos["side"] == "BUY" else "BUY", price / 100.0, pos["size"])
                success = isinstance(res, dict) and not res.get("error")
                if not success:
                    return await q.edit_message_text(f"❌ Не удалось закрыть: `{res}`", parse_mode="Markdown", reply_markup=KB([back("tr_back")]))
            remove_position(pid)
            pnl = round(((price - pos["entry_price"]) if pos["side"] == "BUY" else (pos["entry_price"] - price)) * pos["size"] / 100.0, 2)
            add_trade_history(pos["is_demo"], pos["slug"], pos["question"], pos["outcome"], pos["side"], pos["size"], pos["entry_price"], price, pnl)
            return await q.edit_message_text(f"✅ Позиция закрыта по {price}¢. PnL: {'+' if pnl>0 else ''}{pnl}$", reply_markup=KB([back("tr_back")]))

        if d.startswith("pos_sl0_"):
            pid = int(d[len("pos_sl0_"):])
            pos = get_position_by_id(pid)
            if pos: update_position_limits(pid, 0, pos["tp"])
            return await q.edit_message_text("✅ Stop Loss снят.", reply_markup=KB([back("tr_back")]))

        if d.startswith("pos_tp100_"):
            pid = int(d[len("pos_tp100_"):])
            pos = get_position_by_id(pid)
            if pos: update_position_limits(pid, pos["sl"], 100)
            return await q.edit_message_text("✅ Take Profit выставлен на 100¢.", reply_markup=KB([back("tr_back")]))

        if d.startswith("pos_forget_"):
            pid = int(d[len("pos_forget_"):])
            remove_position(pid)
            return await q.edit_message_text("🗑 Убрано из трекера (сама позиция на Polymarket не тронута).", reply_markup=KB([back("tr_back")]))

        if d.startswith("trc_"):
            idx = d[4:]
            order_id = s.get("cancel_map", {}).get(idx)
            if not order_id:
                return await q.edit_message_text("❌ Не найден ID ордера (обновите список позиций).", reply_markup=KB([back("tr_back")]))
            import polymarket_trading as pt
            res = pt.cancel_order(order_id)
            if isinstance(res, dict) and res.get("error"):
                return await q.edit_message_text(f"❌ Не удалось отменить: `{res['error']}`", parse_mode="Markdown", reply_markup=KB([back("tr_back")]))
            return await q.edit_message_text("✅ Ордер отменён.", reply_markup=KB([back("tr_back")]))

        if d.startswith("smi_"):
            set_setting("m_interval", d[4:])
            schedule_jobs(context, cid)
            return await q.edit_message_text("✅", reply_markup=settings_kb())

        if d.startswith("smt_"):
            set_setting("m_threshold", d[4:])
            return await q.edit_message_text("✅", reply_markup=settings_kb())

    except BadRequest as e:
        # Telegram rejects an edit when a status button is pressed twice and
        # the text/keyboard have not changed. This is harmless, not a server error.
        if "Message is not modified" in str(e):
            return
        log.exception(f"Telegram callback error: {e}")
        await _send_internal_error(update)
    except Exception as e:
        log.exception(f"on_callback error: {e}")
        await _send_internal_error(update)


# ===================== ФОНОВЫЕ ЗАДАЧИ РЫНКОВ =====================
async def job_markets(context: ContextTypes.DEFAULT_TYPE):
    try:
        cid = context.job.data.get("cid")
        if not cid: return

        mth = float(get_setting("m_threshold", "2.0"))

        for gm in get_markets():
            if not gm.get("enabled"): continue
            md = fetch_market(gm["slug"])
            if not md or not md.get("options"): continue

            probs = {str(o.get("label", "")).strip(): o.get("prob") for o in md["options"] if o.get("prob") is not None}
            if not probs: continue
            add_market_history(gm["id"], probs)

            last_probs = gm.get("last_probs") or {}
            if get_setting("market_notifications", "1") == "1" and threshold_exceeded(last_probs, md, mth):
                try: await context.bot.send_message(cid, f"📊 {gm['name']} изменился\n{build_trend(last_probs, md)}")
                except: pass

            update_market(gm["id"], last_probs=probs)

        # Проверка SL / TP
        positions = get_positions()
        if positions:
            import polymarket_trading as pt
            for pos in positions:
                info = pt.get_event_markets(pos["slug"])
                if not info: continue
                token_price = next((m.get("price_yes") if m.get("token_yes") == pos["token_id"] else m.get("price_no") for m in info.get("markets", []) if pos["token_id"] in (m.get("token_yes"), m.get("token_no"))), None)
                if token_price is None: continue

                trigger = False
                if pos["side"] == "BUY":
                    if (pos["tp"] > 0 and token_price >= pos["tp"]) or (pos["sl"] > 0 and token_price <= pos["sl"]): trigger = True
                else:
                    if (pos["tp"] > 0 and token_price <= pos["tp"]) or (pos["sl"] > 0 and token_price >= pos["sl"]): trigger = True

                if trigger:
                    success = True
                    if pos["is_demo"] == 0:
                        res = pt.place_order(pos["token_id"], "SELL" if pos["side"] == "BUY" else "BUY", token_price / 100.0, pos["size"])
                        success = isinstance(res, dict) and not res.get("error")
                        if not success and any(err in str(res.get("error", "")).lower() for err in ["balance", "insufficient", "position"]):
                            remove_position(pos["id"])
                            continue

                    if success:
                        remove_position(pos["id"])
                        pnl = round(((token_price - pos["entry_price"]) if pos["side"] == "BUY" else (pos["entry_price"] - token_price)) * pos["size"] / 100.0, 2)
                        add_trade_history(pos["is_demo"], pos["slug"], pos["question"], pos["outcome"], pos["side"], pos["size"], pos["entry_price"], token_price, pnl)
                        try: await context.bot.send_message(cid, f"🔔 Автозакрытие сработало!\nЗакрыто по: {token_price}¢\nPnL: {'+' if pnl > 0 else ''}{pnl}$")
                        except: pass

    except Exception as e:
        log.exception(f"job_markets error: {e}")


async def job_liq_signal(context: ContextTypes.DEFAULT_TYPE):
    try:
        await ls.scan_for_signal(context)
    except Exception as e:
        log.exception(f"job_liq_signal error: {e}")


async def job_liq_position(context: ContextTypes.DEFAULT_TYPE):
    try:
        await ls.scan_open_position(context)
    except Exception as e:
        log.exception(f"job_liq_position error: {e}")


def schedule_jobs(context, cid=None):
    jq = getattr(context, "job_queue", None) or getattr(context, "application", None) and context.application.job_queue
    if not jq:
        return

    if not cid:
        return
    names = [f"{prefix}:{cid}" for prefix in ("mk_job", "liq_signal_job", "liq_position_job")]
    for name in names:
        for j in jq.get_jobs_by_name(name):
            j.schedule_removal()

    mi = int(get_setting("m_interval", "30"))
    jq.run_repeating(job_markets, interval=mi, first=15, name=names[0], data={"cid": cid})

    # У стратегии ликвидаций два независимых цикла:
    # - поиск сигнала (интервал проверки ликвидаций)
    # - слежение за уже открытой позицией (время сканирования, обычно чаще)
    li_check = int(float(get_setting("liq_check_interval", ls.DEFAULTS["liq_check_interval"])))
    li_scan = int(float(get_setting("liq_scan_interval", ls.DEFAULTS["liq_scan_interval"])))
    jq.run_repeating(job_liq_signal, interval=max(li_check, 1), first=5, name=names[1], data={"cid": cid})
    jq.run_repeating(job_liq_position, interval=max(li_scan, 1), first=5, name=names[2], data={"cid": cid})
