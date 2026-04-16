from markets import get_markets_info
from top_wallets_processor import run_top_wallets_ingestion

CATEGORIES_TAG = ["politics", "geopolitics", "tech", "finance", "economy"]

all_market_ids = []
"""for tag in CATEGORIES_TAG:
    print(f"\n[✓] Starting data ingestion for category: {tag}")
    market_ids = list(get_markets_info(tag).keys())
    all_market_ids.extend(market_ids)

# Evita duplicados si un mismo market aparece en varias categorías.
all_market_ids = list(dict.fromkeys(all_market_ids))
print(f"\n[✓] Total unique market IDs: {len(all_market_ids)}")"""
market_ids = list(get_markets_info("finance").keys())[:20]
print(f"\n[✓] Total unique market IDs (first 20 for testing): {len(market_ids)}")
run_top_wallets_ingestion(market_ids)