import json
import random
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from .logger import get_logger
from .kafka_managerV2 import get_producer
from .markets import get_markets_info, obtain_event_data
from .rate_limiter import RateLimiter

log = get_logger(__name__)

api_rate_limiter = RateLimiter(max_calls=900, period=10.0, min_interval=0.012)


def _process_single_market(session, condition_id):
    """
    Worker function: Fetches the top holders for a single market condition,
    handles rate limits/server errors with exponential backoff, and sends
    the data to Kafka.
    """
    url = "https://data-api.polymarket.com/holders"
    params = {
        "market": condition_id,
        "limit": 5,
    }

    max_retries = 3

    for attempt in range(max_retries):
        try:
            api_rate_limiter.wait()
            response = session.get(url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                try:
                    producer = get_producer()
                    if producer:
                        producer.send_data(
                            topic="top_wallets", data=data, key=condition_id
                        )
                except Exception as e:
                    log.error("Error sending market %s to Kafka: %s", condition_id, e)

                return condition_id, data

            elif response.status_code == 400:
                log.error("[400] Bad Request for market %s. Check parameters.", condition_id)
                return None

            elif response.status_code == 401:
                log.error("[401] Unauthorized for market %s. Check API Key.", condition_id)
                return None

            elif response.status_code in [408, 429, 500, 502, 503, 504]:
                wait = (2 ** attempt) + random.uniform(0, 1)
                if response.status_code == 429:
                    log.warning(
                        "[429] Too many requests. Throttling market %s for %.2fs...",
                        condition_id, wait
                    )
                else:
                    log.warning(
                        "[%d] Server error. Retrying market %s in %.2fs...",
                        response.status_code, condition_id, wait
                    )
                time.sleep(wait)
                continue

            else:
                log.error(
                    "Unexpected HTTP %d for market %s",
                    response.status_code, condition_id
                )
                return None

        except requests.exceptions.RequestException as e:
            wait = (2 ** attempt) + random.uniform(0, 1)
            log.error(
                "Connection error for market %s: %s. Retrying in %.2fs...",
                condition_id, e, wait
            )
            time.sleep(wait)
        except ValueError:
            log.error("Error decoding JSON response for market %s", condition_id)
            return None

    log.error("Max retries reached for market %s after server failures", condition_id)
    return None


def run_top_wallets_ingestion(session, condition_ids, max_workers=10):
    """
    Orchestrates the concurrent ingestion of top wallet holders for all given markets.
    """
    all_wallets = {}
    total = len(list(condition_ids))
    log.info("Starting top wallets ingestion for %d markets (workers=%d)", total, max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_process_single_market, session, c_id): c_id
            for c_id in condition_ids
        }

        for idx, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result(timeout=60)
                if result:
                    c_id, data = result
                    all_wallets[c_id] = data
            except Exception as e:
                c_id = futures[future]
                log.error("[%d/%d] Error in market %s: %s", idx, total, c_id, e)

    log.info("Top wallets ingestion complete. Processed %d markets.", len(all_wallets))
    return extract_unique_wallets(all_wallets)


def extract_unique_wallets(all_markets_top_wallets):
    """
    Receives the compiled top wallets dictionary, extracts all unique
    user addresses, and saves them to a JSON file for further profile enrichment.
    """
    unique_wallets = set()

    for market_id, token_list in all_markets_top_wallets.items():
        for token_data in token_list:
            for holder in token_data.get("holders", []):
                address = holder.get("proxyWallet")
                if address and address.startswith("0x"):
                    unique_wallets.add(address)

    unique_file_name = "data_ingestion/unique_wallets_list.json"
    unique_data = {
        "total_unique_wallets": len(unique_wallets),
        "wallet_addresses": list(unique_wallets),
    }

    with open(unique_file_name, "w", encoding="utf-8") as f:
        json.dump(unique_data, f, indent=2, ensure_ascii=False)

    log.info(
        "Unique wallets saved to '%s'. Total: %d",
        unique_file_name, len(unique_wallets)
    )
    return unique_data["wallet_addresses"]


if __name__ == "__main__":
    http_session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    http_session.mount("https://", adapter)
    http_session.mount("http://", adapter)

    MAX_WORKERS = 20
    CATEGORIES_TAG = ["economy"]

    market_mapping, _ = obtain_event_data(http_session, CATEGORIES_TAG)
    all_market_ids = market_mapping.keys()
    run_top_wallets_ingestion(http_session, all_market_ids, MAX_WORKERS)
    get_producer().flush()
