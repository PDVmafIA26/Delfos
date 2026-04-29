import os
from pathlib import Path

# Polimarket configuration
POLIMARKET_BASE_URL = "https://polymarket.com"

# Telegram Bot API configuration
BOT_TOKEN_TELEGRAM = os.getenv("TELEGRAM_BOT_TOKEN", "undefined_token")
BASE_URL_TELEGRAM = f"https://api.telegram.org/bot{BOT_TOKEN_TELEGRAM}"
CHAT_ID_TELEGRAM = os.getenv("TELEGRAM_CHAT_ID", "undefined_chat_id")

# Discord Webhook configuration
BASE_URL_DISCORD = os.getenv("DISCORD_URL_WEBHOOK", "undefined_url")


DEFAULT_IMAGE_DIR = "assets/default_images"
DEFAULT_IMAGES = {
    "FLIP": f"{DEFAULT_IMAGE_DIR}/flip.jpg",  # Flip Anomaly
    # "WHALE": f"{DEFAULT_IMAGE_DIR}/whale.png", # Whale Movement Anomaly
}

# Base directory for the project
BASE_DIR = Path(__file__).resolve().parent
