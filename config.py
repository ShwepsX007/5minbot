import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "bot_database.db")

# ===== TELEGRAM =====
TOKEN = os.getenv("TOKEN")

# ===== INTERVALS =====
DEFAULT_MARKET_INTERVAL = 30
ALLOWED_MARKET_INTERVALS = (5, 10, 20, 30, 60)

# ===== POLYMARKET =====
POLY_PRIVATE_KEY      = os.getenv("POLY_PRIVATE_KEY")
POLY_API_KEY          = os.getenv("POLY_API_KEY")
POLY_API_SECRET       = os.getenv("POLY_API_SECRET")
POLY_API_PASSPHRASE   = os.getenv("POLY_API_PASSPHRASE")
POLY_FUNDER           = os.getenv("POLY_FUNDER")
try:
    POLY_SIGNATURE_TYPE = int(os.getenv("POLY_SIGNATURE_TYPE", "3"))
except ValueError:
    POLY_SIGNATURE_TYPE = 3

# Comma-separated Telegram user IDs permitted to control the bot.  The same
# admins work in a private chat and in groups where the bot is present.
_raw_admin_ids = os.getenv("ADMIN_CHAT_IDS", "")
ADMIN_CHAT_IDS = frozenset(
    int(value.strip()) for value in _raw_admin_ids.split(",") if value.strip().lstrip("-").isdigit()
)
