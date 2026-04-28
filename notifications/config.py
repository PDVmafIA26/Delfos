import os
from pathlib import Path

try:
    # Development settings: load dotenv to read environment variables from a .env file
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    # In production, we expect environment variables to be set directly, so we can ignore the absence of dotenv.
    pass

# Polimarket configuration
POLIMARKET_BASE_URL = "https://polymarket.com"

# Telegram Bot API configuration
BOT_TOKEN_TELEGRAM = os.getenv("TELEGRAM_BOT_TOKEN", "undefined_token")
BASE_URL_TELEGRAM = f"https://api.telegram.org/bot{BOT_TOKEN_TELEGRAM}"
CHAT_ID_TELEGRAM = os.getenv("TELEGRAM_CHAT_ID", "undefined_chat_id")

# Discord Webhook configuration
BASE_URL_DISCORD = os.getenv("DISCORD_URL_WEBHOOK", "undefined_url")


DEFAULT_IMAGE = "assets/politics.jpg"

# Base directory for the project
BASE_DIR = Path(__file__).resolve().parent
