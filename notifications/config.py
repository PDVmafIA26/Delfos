import os
from pathlib import Path

try:
    # Development settings: load dotenv to read environment variables from a .env file
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    # In production, we expect environment variables to be set directly, so we can ignore the absence of dotenv.
    pass

# Telegram Bot API configuration
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "undefined_token")
BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "undefined_chat_id")

# Base directory for the project
BASE_DIR = Path(__file__).resolve().parent
