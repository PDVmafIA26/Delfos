# from datetime import datetime, timezone
import json
import random
from kafka_manager import get_producer
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def _process_single_market(session, condition_id):
    """ 
    Worker function: Fetches the top holders for a single market condition,
    handles rate limits/server errors with exponential backoff, and sends 
    the data to Kafka.
    """
    url = "https://data-api.polymarket.com/holders"
    params = {
        "market": condition_id,
        "limit": 20,
    }

    max_retries = 3

    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, timeout=10)
            
            # 200 OK: Data retrieved successfully
            if response.status_code == 200:
                data = response.json()
                market_data = {
                    "market_id": condition_id,
                    "top_holders": data
                }

                # Send data to Kafka
                try:
                    get_producer().send_data(
                        topic="top_wallets", data=market_data, key=str(condition_id)
                    )
                    #print(f"Top wallets (Kafka) -> Market {condition_id}")
                except Exception as e:
                    print(f"Error sending market {condition_id} to Kafka: {e}")

                # Return ID and data to save them in the main dictionary                
                return condition_id, data

            # 400 Bad Request: Client-side error, no need to retry
            elif response.status_code == 400:
                print(f"[400] Bad Request en {condition_id}. Check parameters.")
                return None
            
            # 401 Unauthorized: Auth error, no need to retry
            elif response.status_code == 401:
                print(f"[401] Unauthorized en {condition_id}. Check API Key or signatures.")
                return None
            
            # Network/Server temporary errors: Apply exponential backoff and retry
            elif response.status_code in [408, 429, 500, 502, 503, 504]:
                wait = (2 ** attempt) + random.uniform(0, 1)  # Exponential backoff: 1.3s, 2.7s, 1.1s, 4.6s...
                if response.status_code == 429:
                    print(f"[429] Too many requests. THROTTLING market {condition_id} for {wait}s...")
                elif response.status_code == 500:
                    print(f"[500] Internal Server Error. Retrying market {condition_id} in {wait}s...")
                else:
                    print(f"[{response.status_code}] Server Error. Retrying market {condition_id} in {wait}s...")
                
                time.sleep(wait)
                continue # Proceed to the next attempt in the 'for' loop

            # Handle any other undocumented status codes
            else:
                print(f"Unexpected error {response.status_code} for market {condition_id}")
                return None

        except requests.exceptions.RequestException as e:
            wait = (2 ** attempt) + random.uniform(0, 1)
            print(f"Connection error for market {condition_id}: {e}. Retrying in {wait}s...")
            time.sleep(wait)
        except ValueError:
            print(f"Error decoding JSON for market {condition_id}")
            return None
    
    print(f"Max retries reached for market {condition_id} after server failures.")
    return None




def run_top_wallets_ingestion(session, condition_ids, max_workers = 9):
    """
    Orchestrates the concurrent ingestion of top wallet holders for all given markets.
    Fetches data in parallel using a ThreadPoolExecutor, sends each market's results
    to Kafka, and saves the compiled data to disk.
    """

    all_wallets = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_single_market, session, c_id): c_id for c_id in condition_ids}

        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    c_id, data = result
                    all_wallets[c_id] = data
            except Exception as e:
                print(f"Error in the thread: {e}")
    
    # Save to JSON file
    file_name = "top_wallets_data.json"
    with open(file_name, "w", encoding="utf-8") as file:
        json.dump(all_wallets, file, indent=4, ensure_ascii=False)
    print(f"Top wallets data saved to '{file_name}'")

    # Proceed to extract unique wallet addresses
    extract_unique_wallets(all_wallets)



def extract_unique_wallets(all_markets_top_wallets):
    """
    Receives the compiled top wallets dictionary, extracts all unique
    user addresses, and saves them to a JSON file
    for further profile enrichment.
    """
    # Prevent duplicates
    unique_wallets = set()

    # Iterate through each market's data
    for market_id, token_list in all_markets_top_wallets.items():
        for token_data in token_list:
            for holder in token_data.get("holders", []):
        
                address = holder.get("proxyWallet")
                # Only add valid user addresses (starting with 0x)
                if address and address.startswith("0x"):
                    unique_wallets.add(address)
    
    # Save unique wallets list to JSON file for later enrichment
    unique_file_name = "unique_wallets_list.json"
    unique_data = {
        # "generated_at": datetime.now(timezone.utc).isoformat(), # UTC timestamp for reproducibility
        "total_unique_wallets": len(unique_wallets), # Count of unique addresses found
        "wallet_addresses": list(unique_wallets) # Convert set to list for JSON serialization
    }
    
    # Write the unique wallets data to disk
    with open(unique_file_name, "w", encoding="utf-8") as f:
        json.dump(unique_data, f, indent=2, ensure_ascii=False)
    
    # Output confirmation messages for logging/debugging
    print(f"Unique wallets saved to '{unique_file_name}'")
    print(f"{len(unique_wallets)} unique wallets extracted")


