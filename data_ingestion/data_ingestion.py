from markets import obtain_event_data
from top_wallets_processor import run_top_wallets_ingestion
from websocket_ingestor import run_websocket
import threading
import requests
from kafka_manager import get_producer
from wallet_analyzer import WalletAnalyzer


def main():

    CATEGORIES_TAG = ["politics", "geopolitics", "tech", "finance", "economy"]
    OUTPUT_FILE = "data_ingestion/wallets_complete_data.json"
    MAX_WORKERS = 5  # Good balance between speed and safety (due to rate limits)

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
        all_market_ids = market_mapping.keys()
        # Obtains all assets IDs from the markets
        all_assets_ids = [
            token for market in market_mapping.values() for token in market
        ]

        print(f"    - Total markets mapped: {len(market_mapping)}")
        print(f"    - Total events saved: {len(collected_events)}")

        # Fetch top wallets concurrently while WebSocket streams
        wallet_addresses = run_top_wallets_ingestion(http_session, all_market_ids)

        # Velue can be changed

        print("=" * 60)
        print("WALLET ANALYZER")
        print("Fetches profile + trading history for each wallet")
        print("=" * 60)

        # Analyze all wallets
        analyzer = WalletAnalyzer()
        results = analyzer.analyze_multiple_wallets(
            wallet_addresses, max_workers=MAX_WORKERS
        )

        # Save combined results
        analyzer.save_results(results, OUTPUT_FILE)

        # Print summary
        print("\n" + "-" * 40)
        print("SUMMARY")
        print("-" * 40)
        profiles_found = sum(1 for r in results if r["profile"].get("name"))
        total_positions = sum(r["trading"].get("total_positions", 0) for r in results)
        print(f"Total wallets: {len(results)}")
        print(f"Profiles found: {profiles_found}")
        print(f"Total positions: {total_positions}")
        print(f"\nOutput: {OUTPUT_FILE}")

        get_producer().flush()

        # Execute WebSocket in a separate thread to avoid blocking
        websocket_thread = threading.Thread(
            target=run_websocket, args=(all_assets_ids, stop_event)
        )
        websocket_thread.daemon = True
        websocket_thread.start()
        print("WebSocket ingestion started in background.")

        # NOTE: WebSocket is started after top_wallets ingestion completes.
        # This is intentional during development to keep terminal output readable.
        # In production, start the WebSocket first to avoid missing market data
        # while top_wallets ingestion is running.

        while websocket_thread.is_alive():
            websocket_thread.join(timeout=1.0)

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


if __name__ == "__main__":
    main()
