# Combines profile enrichment and trading history
# Generates a single JSON file with complete information per wallet
# Profile data & trading history

import traceback

import requests
import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, List
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
from kafka_managerV2 import get_producer
from rate_limiter import RateLimiter

HISTORY_URL = "https://data-api.polymarket.com/closed-positions"
api_rate_limiter = RateLimiter(max_calls=140, period=10.0, min_interval=0.075)


def fetch_history(
    session,
    wallet_address: str,
) -> Dict[str, Any]:
    """Fetch only the first 50 closed positions for a wallet."""
    positions = []
    max_retries = 3

    # Fetch first page (50 positions)
    params = {
        "user": wallet_address,
        "limit": 50,
        "offset": 0,
        "sortBy": "TIMESTAMP",
        "sortDirection": "DESC",
    }

    page_success = False

    # Retry loop in case the conexion fails
    for attempt in range(max_retries):
        try:
            api_rate_limiter.wait()
            response = session.get(HISTORY_URL, params=params, timeout=10)

            if response.status_code == 200:
                positions = response.json()
                page_success = True
                break  # Success, exit the retry loop

            elif response.status_code == 400:
                print(f"[400] Bad Request en {wallet_address}. Check parameters.")
                break  # Fatal error, exit retry loop

            elif response.status_code == 401:
                print(
                    f"[401] Unauthorized en {wallet_address}. Check API Key or signatures."
                )
                break  # Fatal error, exit retry loop

            # Network/Server temporary errors: Apply exponential backoff and retry
            elif response.status_code in [408, 429, 500, 502, 503, 504]:
                wait = (2**attempt) + random.uniform(0, 1)
                if response.status_code == 429:
                    print(
                        f"[429] Too many requests. THROTTLING history for {wallet_address} for {wait:.2f}s..."
                    )
                    pass
                elif response.status_code == 500:
                    print(
                        f"[500] Internal Server Error. Retrying history for {wallet_address} in {wait:.2f}s..."
                    )
                else:
                    print(
                        f"[{response.status_code}] Server Error. Retrying history for {wallet_address} in {wait:.2f}s..."
                    )

                time.sleep(wait)
                continue  # Proceed to the next attempt in the 'for' loop

            # Handle any other undocumented status codes
            else:
                print(
                    f"Unexpected error {response.status_code} fetching history for {wallet_address}"
                )
                break

        except requests.exceptions.RequestException as e:
            wait = (2**attempt) + random.uniform(0, 1)
            print(
                f"Connection error for history of {wallet_address}: {e}. Retrying in {wait:.2f}s..."
            )
            time.sleep(wait)
        except ValueError:
            print(f"Error decoding JSON for history of wallet {wallet_address}")
            break  # Fatal error, exit retry loop

    # If the retry loop ended without success
    if not page_success:
        if attempt == max_retries - 1:
            print(f"Max retries reached for history of {wallet_address}.")

    # --- Data processing ---
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


def save_results(results: List[Dict[str, Any]], output_path: str) -> bool:
    # Save combined results to JSON file
    try:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        output_data = {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "total_wallets": len(results),
                "successful_fetches": len(results),
            },
            "wallets": results,
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        print(f"\n[✓] Results saved to: {output_path}")
        return True

    except Exception as e:
        print(f"[X] Error saving: {e}")
        return False


def load_wallets_from_file(
    file_path: str = "unique_wallets_list.json",
) -> List[str]:
    # Load wallet addresses from unique_wallets_list.json
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        wallets = data.get("wallet_addresses", {})
        print(f"Loaded {len(wallets)} wallets from {file_path}")
        return wallets

    except Exception as e:
        print(f"[X] Error loading wallets: {e}")
        return []


def run_wallet_analysis_pipeline(
    session,
    wallet_addresses: Dict[str, Any],
    output_file: str = "wallets_complete_data.json",
    max_workers: int = 7,
) -> List[Dict[str, Any]]:
    """
    Orchestrates the concurrent ingestion and analysis of top wallet users data.
    Fetches data in parallel using a ThreadPoolExecutor, sends each user's results
    to Kafka, and saves the compiled data to disk.
    """
    print("=" * 60)
    print("WALLET ANALYZER PIPELINE")
    print("Fetches trading history for each wallet")
    print("=" * 60)

    if not wallet_addresses:
        print("No wallets provided to analyze.")
        return []

    # Analyze all wallets
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
                print(
                    f"[{idx}/{len(wallet_addresses)}] ✗ {wallet_address[:8]}... - error: {e}"
                )
                results.append({})

    # Save results if an output file is specified
    if output_file:
        save_results(results, output_file)

    if output_file:
        print(f"\nOutput saved to: {output_file}")

    return results


def main():

    # Local test entry point
    http_session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    http_session.mount("https://", adapter)
    http_session.mount("http://", adapter)

    INPUT_FILE = "unique_wallets_list.json"
    OUTPUT_FILE = "wallets_complete_data.json"
    MAX_WORKERS = 20  # Good balance between speed and safety (due to rate limits)
    # Value can be changed

    # Load wallets from unique list
    wallet_addresses = load_wallets_from_file(INPUT_FILE)

    if not wallet_addresses:
        print("No wallets found. Run top_wallets_processor.py first.")
        return

    # Analyze all wallets
    run_wallet_analysis_pipeline(
        http_session, wallet_addresses, OUTPUT_FILE, max_workers=MAX_WORKERS
    )
    get_producer().flush()


if __name__ == "__main__":
    main()
