import requests
import time
import random
from typing import Dict, Any, List, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

from logger import get_logger
from rate_limiter import RateLimiter
from kafka_manager import get_producer

log = get_logger(__name__)

HISTORY_URL = "https://data-api.polymarket.com/positions"

api_rate_limiter = RateLimiter(max_calls=140, period=10.0, min_interval=0.075)


def fetch_current_positions(
    session: requests.Session, wallet_address: str
) -> Union[List[Dict[str, Any]], Dict[str, Any], None]:
    """
    Fetches the raw, unedited open positions for a given wallet address from the API.
    Implements a robust retry mechanism with exponential backoff to handle transient
    network issues and rate-limiting (HTTP 429).
    """
    max_retries = 3

    params = {
        "user": wallet_address,
        "limit": 500,
        "sortBy": "RESOLVING",
        "sortDirection": "DESC",
    }

    data = None
    success = False

    for attempt in range(max_retries):
        try:
            api_rate_limiter.wait()
            response = session.get(HISTORY_URL, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                success = True
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
                        "[429] Rate limit exceeded. Throttling %s for %.2fs...",
                        wallet_address, wait
                    )
                else:
                    log.warning(
                        "[%d] Server error. Retrying %s in %.2fs...",
                        response.status_code, wallet_address, wait
                    )
                time.sleep(wait)
                continue

            else:
                log.error(
                    "Unexpected HTTP %d fetching positions for wallet %s",
                    response.status_code, wallet_address
                )
                break

        except requests.exceptions.RequestException as e:
            wait = (2 ** attempt) + random.uniform(0, 1)
            log.error(
                "Connection error for wallet %s: %s. Retrying in %.2fs...",
                wallet_address, e, wait
            )
            time.sleep(wait)

        except ValueError:
            log.error("Error decoding JSON for wallet %s", wallet_address)
            break

    if not success and attempt == max_retries - 1:
        log.error("Max retries reached. Failed to fetch positions for wallet %s", wallet_address)

    return data


def process_wallet(session: requests.Session, wallet_address: str) -> Dict[str, Any]:
    """
    Orchestrates the data retrieval and Kafka ingestion for a single wallet.
    """
    data = fetch_current_positions(session, wallet_address)

    if data is not None and get_producer():
        get_producer().send_data(
            topic="wallet_current_positions",
            data=data,
            key=wallet_address,
        )

    return {
        "wallet_address": wallet_address,
        "status": "success" if data is not None else "failed",
    }


def analyze_multiple_wallets_positions(
    session: requests.Session, wallet_addresses: List[str], max_workers: int = 5
) -> List[Dict[str, Any]]:
    """
    Executes the fetching process concurrently using a ThreadPool.
    """
    results = []
    success_count = 0
    failed_count = 0

    log.info("Fetching positions for %d wallets (workers=%d)", len(wallet_addresses), max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_wallet, session, wallet): wallet
            for wallet in wallet_addresses
        }

        for idx, future in enumerate(as_completed(futures), 1):
            wallet_address = futures[future]
            try:
                result = future.result(timeout=60)
                results.append(result)
                success_count += 1
            except Exception as e:
                failed_count += 1
                log.error(
                    "[%d/%d] Exception for wallet %s...: %s",
                    idx, len(wallet_addresses), wallet_address[:8], e
                )
                results.append({
                    "wallet_address": wallet_address,
                    "status": "error",
                    "error_msg": str(e),
                })

    log.info(
        "Positions pipeline complete. Success: %d | Failed: %d",
        success_count, failed_count
    )
    return results


def main():
    """
    Main entry point for the script.
    """
    http_session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    http_session.mount("https://", adapter)
    http_session.mount("http://", adapter)

    MAX_WORKERS = 20

    wallet_addresses = [
        "0x601d0223fe727ce77ecc47227251e0b102b3f618",
        "0x8c7b26969470bd87494b5d406c3e84256d102a87",
    ]

    if not wallet_addresses:
        log.warning("No wallets found to process. Terminating.")
        return

    analyze_multiple_wallets_positions(http_session, wallet_addresses, max_workers=MAX_WORKERS)

    if get_producer():
        get_producer().flush()
        log.info("All raw messages successfully flushed to Kafka.")


if __name__ == "__main__":
    main()
