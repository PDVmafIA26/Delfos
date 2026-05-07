import requests
import time
import random
from typing import Dict, Any, List, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

# Custom modules for pipeline integration
from rate_limiter import RateLimiter
from kafka_manager import get_producer

# API Endpoint for fetching current positions
HISTORY_URL = "https://data-api.polymarket.com/positions"

# Global rate limiter configured to respect API constraints:
# Max 140 requests per 10-second window, with a minimum 75ms gap between requests.
api_rate_limiter = RateLimiter(max_calls=140, period=10.0, min_interval=0.075)


def fetch_current_positions(
    session: requests.Session, wallet_address: str
) -> Union[List[Dict[str, Any]], Dict[str, Any], None]:
    """
    Fetches the raw, unedited open positions for a given wallet address from the API.
    Implements a robust retry mechanism with exponential backoff to handle transient
    network issues and rate-limiting (HTTP 429).

    Args:
        session: Pre-configured requests.Session object with connection pooling.
        wallet_address: The target crypto wallet address.

    Returns:
        The raw JSON response from the API (usually a list of positions),
        or None if all retry attempts fail.
    """
    max_retries = 3

    # Query parameters based on API specifications
    params = {
        "user": wallet_address,
        "limit": 500,
        "sortBy": "RESOLVING",
        "sortDirection": "DESC",
    }

    data = None
    success = False

    # Retry loop to handle transient API failures
    for attempt in range(max_retries):
        try:
            # Block until a slot is available according to rate limits
            api_rate_limiter.wait()

            # Execute request with a hard timeout to prevent hanging threads
            response = session.get(HISTORY_URL, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                success = True
                break  # Successful fetch, exit retry loop

            elif response.status_code == 400:
                print(f"[400] Bad Request for {wallet_address}. Check parameters.")
                break  # Fatal client error, exit retry loop

            elif response.status_code == 401:
                print(
                    f"[401] Unauthorized for {wallet_address}. Check API Key or signatures."
                )
                break  # Fatal authorization error, exit retry loop

            # Handle recoverable Network/Server errors with exponential backoff
            elif response.status_code in [408, 429, 500, 502, 503, 504]:
                # Calculate backoff: 2^attempt seconds + random jitter to prevent thundering herd
                wait = (2**attempt) + random.uniform(0, 1)

                if response.status_code == 429:
                    print(
                        f"[429] Rate limit exceeded. THROTTLING {wallet_address} for {wait:.2f}s..."
                    )
                else:
                    print(
                        f"[{response.status_code}] Server Error. Retrying {wallet_address} in {wait:.2f}s..."
                    )

                time.sleep(wait)
                continue  # Proceed to the next iteration of the retry loop

            else:
                # Catch-all for undocumented status codes
                print(
                    f"Unexpected error {response.status_code} fetching history for {wallet_address}"
                )
                break

        except requests.exceptions.RequestException as e:
            # Handle connection timeouts, DNS failures, etc.
            wait = (2**attempt) + random.uniform(0, 1)
            print(
                f"Connection error for {wallet_address}: {e}. Retrying in {wait:.2f}s..."
            )
            time.sleep(wait)

        except ValueError:
            # Handle cases where the API returns a 200 OK but malformed JSON
            print(f"Error decoding JSON for wallet {wallet_address}")
            break

    if not success and attempt == max_retries - 1:
        print(f"Max retries reached. Failed to fetch positions for {wallet_address}.")

    return data


def process_wallet(session: requests.Session, wallet_address: str) -> Dict[str, Any]:
    """
    Orchestrates the data retrieval and Kafka ingestion for a single wallet.

    Args:
        session: Active HTTP session.
        wallet_address: Target wallet address.

    Returns:
        A dictionary containing the execution status for monitoring purposes.
    """
    # 1. Fetch raw payload from the Polymarket API
    data = fetch_current_positions(session, wallet_address)

    # 2. Push unmodified data directly to the Kafka broker
    # We check for `is not None` because an empty list `[]` is a valid response (0 positions)
    if data is not None and get_producer():
        get_producer().send_data(
            topic="wallet_current_positions",
            data=data,
            key=wallet_address,  # Key ensures messages for the same wallet go to the same partition
        )

    # 3. Return local execution metadata
    return {
        "wallet_address": wallet_address,
        "status": "success" if data is not None else "failed",
    }


def analyze_multiple_wallets_positions(
    session: requests.Session, wallet_addresses: List[str], max_workers: int = 5
) -> List[Dict[str, Any]]:
    """
    Executes the fetching process concurrently using a ThreadPool.

    Args:
        session: Shared HTTP session.
        wallet_addresses: List of target wallet strings.
        max_workers: Maximum number of concurrent threads.

    Returns:
        List of dictionaries containing execution metadata/logs.
    """
    results = []
    success_count = 0
    failed_count = 0

    print(f"\n{'='*60}")
    print(f"FETCHING POSITIONS FOR {len(wallet_addresses)} WALLETS")
    print(f"{'='*60}\n")

    # Utilize ThreadPoolExecutor for I/O bound concurrency
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Map futures to their respective wallet addresses for error tracking
        futures = {
            executor.submit(process_wallet, session, wallet): wallet
            for wallet in wallet_addresses
        }

        # Process results as they complete (order is not guaranteed)
        for idx, future in enumerate(as_completed(futures), 1):
            wallet_address = futures[future]
            try:
                # Add a reasonable timeout to prevent stuck threads
                result = future.result(timeout=60)
                results.append(result)
                success_count += 1
            except Exception as e:
                failed_count += 1
                print(
                    f"[{idx}/{len(wallet_addresses)}] ✗ {wallet_address[:8]}... - Exception: {e}"
                )
                results.append(
                    {
                        "wallet_address": wallet_address,
                        "status": "error",
                        "error_msg": str(e),
                    }
                )

    return results


def main():
    """
    Main entry point for the script. Initializes the network session,
    loads target data, triggers the concurrent pipeline, and cleans up.
    """
    # Initialize a Session to pool underlying TCP connections.
    # This drastically reduces latency by avoiding SSL handshake overhead on every request.
    http_session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    http_session.mount("https://", adapter)
    http_session.mount("http://", adapter)

    # 20 workers provides good throughput without overwhelming the API
    # considering our 140 req/10s rate limit.
    MAX_WORKERS = 20

    # 1. Get addresses
    wallet_addresses = [
        "0x601d0223fe727ce77ecc47227251e0b102b3f618",
        "0x8c7b26969470bd87494b5d406c3e84256d102a87",
    ]

    if not wallet_addresses:
        print("No wallets found to process. Terminating.")
        return

    # 2. Execute pipeline (Fetch + Push to Kafka)
    analyze_multiple_wallets_positions(
        http_session, wallet_addresses, max_workers=MAX_WORKERS
    )

    # 3. Graceful shutdown
    # Ensures all messages residing in the local producer buffer are delivered
    # to the Kafka broker before the script exits.
    if get_producer():
        get_producer().flush()
        print("\n[✓] All raw messages successfully flushed to Kafka.")


if __name__ == "__main__":
    main()
