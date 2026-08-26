import asyncio
import logging
import os
from logging.handlers import RotatingFileHandler
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from config import TOKEN, BASE_DIR
from database import init_db
import liq_api
import chainlink_price
import orderflow
from bot import cmd_start, on_text, on_callback, schedule_jobs

log_path = os.path.join(BASE_DIR, "bot.log")
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(log_path, maxBytes=2_000_000, backupCount=2, encoding="utf-8"),
    ],
)
# apscheduler по умолчанию логирует КАЖДЫЙ тик ("Running job ... executed successfully"),
# что забивает лог каждую секунду. Поднимем уровень для этого логгера.
logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("main")



async def post_init(application):
    """Start the websocket without JobQueue, which waits indefinitely on stop.

    Also register the per-chat background jobs (market polling, liq
    signal scan, liq position scan) for every admin. Previously these
    were only scheduled when the admin sent /start, which meant a bot
    that started without an admin interaction had no liq_signal_job
    registered at all — the signal scanner would never run.
    """
    # Публичные WS-стримы ликвидаций (без API-ключей):
    #   Bybit   — allLiquidation.<SYMBOL>
    #   Binance — !forceOrder@arr
    #   Gate.io — futures.public_liquidates
    # REST-эндпоинты Binance (/fapi/v1/forceOrders) и Gate (/liq_orders)
    # требуют подписи, поэтому все три биржи работают через WS.
    # Заранее сообщаем WS, какие монеты нужны, чтобы Gate успел
    # подписаться до первого скана, а Binance не копил чужие символы.
    try:
        import liq_strategy as ls
        symbols = ls.get_selected_symbols()
        liq_api.set_symbols(symbols)
        orderflow.set_symbols(symbols)
    except Exception as e:
        log.debug(f"set_symbols on startup failed: {e}")

    listeners = {
        "bybit_liquidations": liq_api.bybit_ws_listener,
        "binance_liquidations": liq_api.binance_ws_listener,
        "gate_liquidations": liq_api.gate_ws_listener,
        # Цена, по которой Polymarket рассчитывает Up/Down: Chainlink TWAP
        # через публичный RTDS Polymarket (ключи не нужны).
        "chainlink_prices": chainlink_price.rtds_listener,
        # Поток сделок Binance (aggTrade) для CVD-фильтра памп/дампа
        "orderflow": orderflow.binance_aggtrade_listener,
    }
    tasks = []
    for name, factory in listeners.items():
        task = asyncio.create_task(factory(), name=name)
        application.bot_data[f"{name}_task"] = task
        tasks.append(task)
        log.info(f"{name} listener task created")
    application.bot_data["liq_ws_tasks"] = tasks

    # Schedule background jobs for every admin. schedule_jobs is safe
    # to call multiple times — it removes existing jobs by name first.
    # ВАЖНО: задачи планируются ОДИН раз, а не на каждого админа. Стратегия
    # одна на весь бот, и несколько наборов джобов означали бы несколько
    # параллельных сканеров одного состояния (дубли сообщений и сделок).
    # Уведомления идут в чат первого админа; после /start в другом чате
    # schedule_jobs перевесит их туда.
    from config import ADMIN_CHAT_IDS
    from bot import schedule_jobs
    admins = sorted(ADMIN_CHAT_IDS)
    if admins:
        try:
            schedule_jobs(application, admins[0])
            log.info(f"Background jobs scheduled once, notifications → {admins[0]}")
            if len(admins) > 1:
                log.info(f"Остальные админы ({len(admins) - 1}) управляют ботом, "
                         f"но своих копий задач не получают")
        except Exception as e:
            log.exception(f"Failed to schedule jobs: {e}")


async def post_shutdown(application):
    """Cancel and await the websocket before systemd's stop timeout expires."""
    tasks = application.bot_data.pop("liq_ws_tasks", [])
    for name in (
        "bybit_liquidations",
        "binance_liquidations",
        "gate_liquidations",
        "chainlink_prices",
        "orderflow",
    ):
        task = application.bot_data.pop(f"{name}_task", None)
        if task and task not in tasks:
            tasks.append(task)
    for task in tasks:
        if task and not task.done():
            task.cancel()
    for task in tasks:
        if not task:
            continue
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.debug(f"listener stop err: {e}")
    log.info("Liquidation WS listeners stopped")

def main():
    if not TOKEN:
        log.error("Telegram TOKEN не найден в конфигурации!")
        return
    from config import ADMIN_CHAT_IDS
    if not ADMIN_CHAT_IDS:
        log.error("ADMIN_CHAT_IDS не задан: бот не будет принимать команды ради безопасности.")
        return

    # Инициализация базы данных
    init_db()

    # Сборка приложения Telegram бота
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()

    # Регистрация обработчиков
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_handler(CallbackQueryHandler(on_callback))

    # Фоновые задачи (опрос рынков, SL/TP, сигналы ликвидаций) запускаются
    # для каждого чата отдельно из cmd_start() — там известен chat_id.

    log.info("Бот запущен и ожидает сообщения...")
    app.run_polling()

if __name__ == "__main__":
    main()
