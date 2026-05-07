import time
import threading
import requests

from logger import get_logger
from markets import obtain_event_data
from top_wallets_processor import run_top_wallets_ingestion
from websocket_ingestor import run_websocket
from kafka_manager import get_producer
from wallet_analyzer import run_wallet_analysis_pipeline

log = get_logger(__name__)


def main():
    start_time = time.time()

    CATEGORIES_TAG = ["politics", "geopolitics", "tech", "finance", "economy"]

    log.info("=== Delfos Data Ingestion — starting pipeline ===")
    log.info("Categories to ingest: %s", CATEGORIES_TAG)

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

        all_market_ids = market_mapping.keys()
        all_assets_ids = [
            token for market in market_mapping.values() for token in market
        ]

        log.info("Total markets mapped: %d", len(market_mapping))
        log.info("Total events saved: %d", len(collected_events))

        # Execute WebSocket in a separate thread to avoid blocking
        websocket_thread = threading.Thread(
            target=run_websocket, args=(all_assets_ids, stop_event)
        )
        websocket_thread.daemon = True
        websocket_thread.start()
        log.info("WebSocket ingestion started in background")

        log.info("Starting top wallets ingestion and analysis...")
        wallet_data = run_top_wallets_ingestion(http_session, all_market_ids)
        wallet_history_data = run_wallet_analysis_pipeline(http_session, wallet_data)
        get_producer().flush()

        while websocket_thread.is_alive():
            websocket_thread.join(timeout=1.0)

    except KeyboardInterrupt:
        log.info("Shutdown signal detected. Starting graceful shutdown...")

    except Exception as e:
        log.exception("Unexpected error in main pipeline: %s", e)
        raise

    finally:
        log.info("Closing HTTP connections...")
        http_session.close()

        if websocket_thread and websocket_thread.is_alive():
            if hasattr(stop_event, "ws"):
                stop_event.ws.close()
            stop_event.set()
            websocket_thread.join(timeout=5.0)

        log.info("Flushing remaining Kafka messages...")
        get_producer().flush()

        elapsed = time.time() - start_time
        minutes, seconds = divmod(elapsed, 60)
        log.info(
            "=== Pipeline shutdown complete. Total time: %d min %.2f sec ===",
            int(minutes), seconds
        )


if __name__ == "__main__":
    main()
