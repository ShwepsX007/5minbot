import asyncio
import logging
import os
from logging.handlers import RotatingFileHandler
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from config import TOKEN, BASE_DIR
from database import init_db
import liq_api
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
    task = asyncio.create_task(liq_api.bybit_ws_listener(), name="bybit_liquidations")
    application.bot_data["bybit_liquidations_task"] = task
    log.info("Bybit liquidation listener task created")

    # Schedule background jobs for every admin. schedule_jobs is safe
    # to call multiple times — it removes existing jobs by name first.
    from config import ADMIN_CHAT_IDS
    from bot import schedule_jobs
    for admin_id in ADMIN_CHAT_IDS:
        try:
            schedule_jobs(application, admin_id)
            log.info(f"Background jobs scheduled for admin {admin_id}")
        except Exception as e:
            log.exception(f"Failed to schedule jobs for admin {admin_id}: {e}")


async def post_shutdown(application):
    """Cancel and await the websocket before systemd's stop timeout expires."""
    task = application.bot_data.pop("bybit_liquidations_task", None)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    log.info("Bybit liquidation listener stopped")

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
