# Combines profile enrichment and trading history
# Fetches profile data & trading history and sends it to Kafka.

import requests
import json
import time
import random
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from logger import get_logger
from kafka_manager import get_producer
from rate_limiter import RateLimiter

log = get_logger(__name__)

HISTORY_URL = "https://data-api.polymarket.com/closed-positions"
api_rate_limiter = RateLimiter(max_calls=140, period=10.0, min_interval=0.075)


def fetch_history(session, wallet_address: str) -> Dict[str, Any]:
    """Fetch only the first 50 closed positions for a wallet."""
    positions = []
    max_retries = 3

    params = {
        "user": wallet_address,
        "limit": 50,
        "offset": 0,
        "sortBy": "TIMESTAMP",
        "sortDirection": "DESC",
    }

    page_success = False

    for attempt in range(max_retries):
        try:
            api_rate_limiter.wait()
            response = session.get(HISTORY_URL, params=params, timeout=10)

            if response.status_code == 200:
                positions = response.json()
                page_success = True
                break

            elif response.status_code == 400:
                log.error("[400] Bad Request for wallet %s. Check parameters.", wallet_address)
                break

            elif response.status_code == 401:
                log.error("[401] Unauthorized for wallet %s. Check API Key.", wallet_address)
                break

            elif response.status_code in [408, 429, 500, 502, 503, 504]:
                wait = (2 ** attempt) + random.uniform(0, 1)
                if response.status_code == 429:
                    log.warning(
                        "[429] Too many requests. Throttling history for %s for %.2fs...",
                        wallet_address, wait
                    )
                else:
                    log.warning(
                        "[%d] Server error. Retrying history for %s in %.2fs...",
                        response.status_code, wallet_address, wait
                    )
                time.sleep(wait)
                continue

            else:
                log.error(
                    "Unexpected HTTP %d fetching history for wallet %s",
                    response.status_code, wallet_address
                )
                break

        except requests.exceptions.RequestException as e:
            wait = (2 ** attempt) + random.uniform(0, 1)
            log.error(
                "Connection error fetching history for %s: %s. Retrying in %.2fs...",
                wallet_address, e, wait
            )
            time.sleep(wait)
        except ValueError:
            log.error("Error decoding JSON for history of wallet %s", wallet_address)
            break

    if not page_success and attempt == max_retries - 1:
        log.error("Max retries reached for history of wallet %s", wallet_address)

    if not positions:
        return {}

    return positions


def analyze_wallet(session, wallet_address: str) -> Dict[str, Any]:
    history = fetch_history(session, wallet_address)

    if get_producer():
        get_producer().send_data(
            topic="user_info",
            data=history,
            key=wallet_address,
        )

    return history


def load_wallets_from_file(
    file_path: str = "data_ingestion/unique_wallets_list.json",
) -> List[str]:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        wallets = data.get("wallet_addresses", {})
        log.info("Loaded %d wallets from %s", len(wallets), file_path)
        return wallets

    except Exception as e:
        log.error("Error loading wallets from %s: %s", file_path, e)
        return []


def run_wallet_analysis_pipeline(
    session,
    wallet_addresses: Dict[str, Any],
    max_workers: int = 7,
) -> List[Dict[str, Any]]:
    """
    Orchestrates the concurrent ingestion and analysis of top wallet users data.
    """
    log.info("=== Wallet Analyzer Pipeline ===")
    log.info("Fetching trading history for %d wallets (workers=%d)", len(wallet_addresses), max_workers)

    if not wallet_addresses:
        log.warning("No wallets provided to analyze.")
        return []

    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(analyze_wallet, session, wallet_address): wallet_address
            for wallet_address in wallet_addresses
        }

        for idx, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result(timeout=60)
                results.append(result)
            except Exception as e:
                wallet_address = futures[future]
                log.error(
                    "[%d/%d] Error analyzing wallet %s...: %s",
                    idx, len(wallet_addresses), wallet_address[:8], e
                )
                results.append({})

    log.info("Wallet analysis complete. %d/%d processed.", len(results), len(wallet_addresses))
    return results


def main():
    http_session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    http_session.mount("https://", adapter)
    http_session.mount("http://", adapter)

    INPUT_FILE = "data_ingestion/unique_wallets_list.json"
    MAX_WORKERS = 20

    wallet_addresses = load_wallets_from_file(INPUT_FILE)

    if not wallet_addresses:
        log.warning("No wallets found. Run top_wallets_processor.py first.")
        return

    run_wallet_analysis_pipeline(http_session, wallet_addresses, max_workers=MAX_WORKERS)
    get_producer().flush()


if __name__ == "__main__":
    main()
