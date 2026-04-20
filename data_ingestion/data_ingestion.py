from markets import get_markets_info, get_tag_ids_by_slug_list
from top_wallets_processor import run_top_wallets_ingestion
from websocket_kafka import run_websocket
import threading
import requests

CATEGORIES_TAG = ["politics", "geopolitics", "tech", "finance", "economy"]

# Configurar sesión HTTP
http_session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
http_session.mount('https://', adapter)
http_session.mount('http://', adapter)

categories = get_tag_ids_by_slug_list(http_session, CATEGORIES_TAG)

ids_categories_exclude = []
all_market_ids = []
all_assets_ids = []
for tag in CATEGORIES_TAG:
    print(f"\n[✓] Starting data ingestion for category: {tag}")
    mapping = get_markets_info(http_session, tag, ids_categories_exclude, generate_json=False)
    market_ids = list(mapping.keys())
    all_market_ids.extend(market_ids)
    all_assets_ids.extend([token for market in mapping.values() for token in market])
    if categories[tag]:
        ids_categories_exclude.append(categories[tag])

# Evita duplicados si un mismo market aparece en varias categorías.
all_market_ids = list(dict.fromkeys(all_market_ids))
all_assets_ids = list(dict.fromkeys(all_assets_ids))
print(f"\n[✓] Total unique market IDs: {len(all_market_ids)}")
print(f"[✓] Total unique assets IDs: {len(all_assets_ids)}")

# Ejecutar ingestion de top wallets
run_top_wallets_ingestion(all_market_ids)

# Ejecutar WebSocket en un hilo separado para no bloquear
websocket_thread = threading.Thread(target=run_websocket, args=(all_assets_ids,))
websocket_thread.daemon = True
websocket_thread.start()

print("WebSocket ingestion started in background.")

http_session.close()

# Mantener el script principal corriendo
websocket_thread.join()