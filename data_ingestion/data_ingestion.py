from markets import obtain_event_data
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

market_mapping, collected_events = obtain_event_data(http_session, CATEGORIES_TAG)

# Obtains conditions IDs from the markets
all_market_ids = market_mapping.keys()
# Obtains all assets IDs from the markets
all_assets_ids = [token for market in market_mapping.values() for token in market]

print(f"    - Total markets mapped: {len(market_mapping)}")
print(f"    - Total events saved: {len(collected_events)}")

# Execute ingestion of top wallets from all markets
run_top_wallets_ingestion(all_market_ids)

# Execute WebSocket in a separate thread to avoid blocking
websocket_thread = threading.Thread(target=run_websocket, args=(all_assets_ids,))
websocket_thread.daemon = True
websocket_thread.start()

print("WebSocket ingestion started in background.")

http_session.close()

# Keep the main script running
websocket_thread.join()