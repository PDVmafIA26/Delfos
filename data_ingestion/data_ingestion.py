import time
from markets import obtain_event_data
from top_wallets_processor import run_top_wallets_ingestion
from websocket_ingestor import run_websocket
import threading
import requests
from kafka_manager import get_producer
from wallet_analyzer import run_wallet_analysis_pipeline


def main():
    start_time = time.time()

    CATEGORIES_TAG = ["politics", "geopolitics", "tech", "finance", "economy"]

    # Configurar sesión HTTP
    http_session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    http_session.mount("https://", adapter)
    http_session.mount("http://", adapter)

    stop_event = threading.Event()
    websocket_thread = None

    try:

        market_mapping, collected_events = obtain_event_data(
            http_session, CATEGORIES_TAG
        )

        # Obtains conditions IDs from the markets
        all_market_ids = market_mapping
        all_market_ids = dict(list(all_market_ids.items())[:1000])
        all_market_ids = all_market_ids.keys()
        # Obtains all assets IDs from the markets
        all_assets_ids = [
            token for market in market_mapping.values() for token in market
        ]

        print(f"    - Total markets mapped: {len(market_mapping)}")
        print(f"    - Total events saved: {len(collected_events)}")

        # Execute WebSocket in a separate thread to avoid blocking
        # websocket_thread = threading.Thread(
        #     target=run_websocket, args=(all_assets_ids, stop_event)
        # )
        # websocket_thread.daemon = True
        # websocket_thread.start()
        # print("WebSocket ingestion started in background.")

        print("Starting top wallets ingestion and analysis...")
        # Fetch top wallets concurrently while WebSocket streams
        wallet_data = run_top_wallets_ingestion(http_session, all_market_ids)
        # Analyze top wallets data
        wallet_history_data = run_wallet_analysis_pipeline(http_session, wallet_data[:1000])
        get_producer().flush()

        # while websocket_thread.is_alive():
        #     websocket_thread.join(timeout=1.0)

    except KeyboardInterrupt:
        print("\nShutdown signal detected. Starting graceful shutdown...")

    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
    finally:
        print("Closing HTTP connections...")
        http_session.close()

        if websocket_thread and websocket_thread.is_alive():
            if hasattr(stop_event, "ws"):
                stop_event.ws.close()
            stop_event.set()
            websocket_thread.join(timeout=5.0)

        print("Flushing remaining Kafka messages...")
        get_producer().flush()

        print("Shutdown complete.")

        end_time = time.time()
        elapsed_time = end_time - start_time
        minutes, seconds = divmod(elapsed_time, 60)

        print(f"total execution time: {int(minutes)} min y {seconds:.2f} seg")


if __name__ == "__main__":
    main()
