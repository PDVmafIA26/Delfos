import requests
import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Input: reads wallet addresses from 'data_ingestion/enriched_wallets_profiles.json'
# Output: generates 'data_ingestion/users_history.json' with:
# - Total positions per user
# - Summary (total won, lost, net PnL)
# - Detailed list of all closed positions

class UserHistoryFetcher:
    
    def __init__(self, base_url: str = "https://data-api.polymarket.com"):
        self.base_url = base_url
        self.closed_positions_endpoint = f"{base_url}/closed-positions"
        self.cache: Dict[str, Dict[str, Any]] = {}
    
    def fetch_closed_positions(
        self,
        wallet_address: str,
        limit_per_page: int = 100,
        max_retries: int = 3
    ) -> Optional[List[Dict[str, Any]]]: # This function returns a list of dictionaries (or None if it fails)
        all_positions = []
        offset = 0
        page = 1 # Uses pagination to fetch all positions
        
        print(f"  Fetching history for {wallet_address[:8]}...")
        
        # Loop stops when API returns fewer items than 'limit_per_page'
        while True:
            params = {
                "user": wallet_address,
                "limit": limit_per_page,
                "offset": offset,
                "sortBy": "REALIZEDPNL",
                "sortDirection": "DESC"
            }
            
            for attempt in range(max_retries):
                try:
                    response = requests.get(
                        self.closed_positions_endpoint,
                        params=params,
                        timeout=10
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if not data:
                            # No more positions
                            return all_positions
                        
                        all_positions.extend(data)
                        print(f"    Page {page}: fetched {len(data)} positions (total: {len(all_positions)})")
                        
                        # If we got fewer than limit, this is the last page
                        if len(data) < limit_per_page:
                            return all_positions
                        
                        offset += limit_per_page
                        page += 1
                        break  # Success, exit retry loop
                        
                    elif response.status_code == 429:
                        # Exponential backoff: wait 1s, 2s, 4s... before retrying
                        # This respects Polymarket's rate limits and prevents blocking
                        wait_time = 2 ** attempt
                        print(f"    Rate limit (429), waiting {wait_time}s...")
                        time.sleep(wait_time)
                        
                    else:
                        print(f"    HTTP {response.status_code} for {wallet_address[:8]}...")
                        return None
                        
                except requests.exceptions.RequestException as e:
                    print(f"    Error (attempt {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(1)
                    else:
                        return None
        
        return all_positions
    
    def process_positions(self, positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process raw positions into a cleaner format with calculated fields.
        """
        processed = []
        
        for pos in positions:
            realized_pnl = float(pos.get("realizedPnl", 0))
            
            # Determine status based on PnL
            if realized_pnl > 0:
                status = "WON"
                amount_display = f"+${realized_pnl:,.2f}"
            elif realized_pnl < 0:
                status = "LOST"
                amount_display = f"-${abs(realized_pnl):,.2f}"
            else:
                status = "TIE"
                amount_display = "$0.00"
            
            processed.append({
                "market_id": pos.get("conditionId"),
                "market_title": pos.get("title", "Unknown"),
                "outcome": pos.get("outcome", "N/A"),
                "realized_pnl": realized_pnl,
                "amount_display": amount_display,
                "status": status,
                "closed_at": pos.get("closedAt"),
                "position_id": pos.get("positionId")
            })
        
        return processed
    
    def get_user_history(self, wallet_address: str) -> Optional[Dict[str, Any]]:
        """
        Get complete history for a single wallet.
        """
        if wallet_address in self.cache:
            return self.cache[wallet_address]
        
        raw_positions = self.fetch_closed_positions(wallet_address)
        
        if raw_positions is None:
            result = {
                "wallet_address": wallet_address,
                "status": "failed",
                "total_positions": 0,
                "summary": {"total_won": 0, "total_lost": 0, "net_pnl": 0},
                "positions": []
            }
            self.cache[wallet_address] = result
            return result
        
        processed_positions = self.process_positions(raw_positions)
        
        # Calculate summary
        total_won = sum(p["realized_pnl"] for p in processed_positions if p["realized_pnl"] > 0)
        total_lost = sum(p["realized_pnl"] for p in processed_positions if p["realized_pnl"] < 0)
        
        result = {
            "wallet_address": wallet_address,
            "status": "success",
            "total_positions": len(processed_positions),
            "summary": {
                "total_won": round(total_won, 2),
                "total_lost": round(abs(total_lost), 2),
                "net_pnl": round(total_won + total_lost, 2)
            },
            "positions": processed_positions
        }
        
        self.cache[wallet_address] = result
        return result
    
    def get_multiple_users_history(
        self,
        wallet_addresses: List[str],
        max_workers: int = 5,
        delay_between_requests: float = 0.2
    ) -> List[Dict[str, Any]]:
        """
        Fetch history for multiple wallets in parallel.
        """
        results = []
        success_count = 0
        failed_count = 0
        
        print(f"\n{'='*60}")
        print(f"FETCHING HISTORY FOR {len(wallet_addresses)} WALLETS")
        print(f"{'='*60}\n")
        
        def fetch_single(wallet):
            return self.get_user_history(wallet)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_single, wallet): wallet for wallet in wallet_addresses}
            
            for idx, future in enumerate(as_completed(futures), 1):
                wallet = futures[future]
                try:
                    result = future.result(timeout=60)
                    results.append(result)
                    
                    if result["status"] == "success":
                        success_count += 1
                        print(f"[{idx}/{len(wallet_addresses)}] ✓ {wallet[:8]}... - {result['total_positions']} positions")
                    else:
                        failed_count += 1
                        print(f"[{idx}/{len(wallet_addresses)}] ✗ {wallet[:8]}... - failed")
                        
                except Exception as e:
                    failed_count += 1
                    print(f"[{idx}/{len(wallet_addresses)}] ✗ {wallet[:8]}... - error: {e}")
                
                time.sleep(delay_between_requests)
        
        return results
    
    def save_to_json(self, data: List[Dict[str, Any]], output_path: str) -> bool:
        """
        Save history data to JSON file.
        """
        try:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            output_data = {
                "metadata": {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "total_wallets": len(data),
                    "successful_fetches": sum(1 for d in data if d["status"] == "success"),
                    "failed_fetches": sum(1 for d in data if d["status"] == "failed")
                },
                "users": data
            }
            
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            print(f"\n[✓] Data saved to: {output_path}")
            return True
            
        except Exception as e:
            print(f"[X] Error saving to file: {e}")
            return False


def load_wallets_from_enriched_file(file_path: str = "data_ingestion/enriched_wallets_profiles.json") -> List[str]:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        wallets = [w["wallet_address"] for w in data.get("wallets", [])]
        print(f"Loaded {len(wallets)} wallets from {file_path}")
        return wallets
        
    except Exception as e:
        print(f"Error loading wallets: {e}")
        return []


def main():
    INPUT_FILE = "data_ingestion/enriched_wallets_profiles.json"
    OUTPUT_FILE = "data_ingestion/users_history.json"
    MAX_WORKERS = 5 # Good balance between speed and safety (due to rate limits)
    DELAY_BETWEEN_REQUESTS = 0.2
    
    print("=" * 60)
    print("USER HISTORY FETCHER")
    print("Fetching closed positions history for multiple wallets")
    print("=" * 60)
    
    # Load wallets from enriched file
    wallet_addresses = load_wallets_from_enriched_file(INPUT_FILE)
    
    if not wallet_addresses:
        print("No wallets found. Run enrich_wallets_profiles.py first.")
        return
    
    # Fetch history
    fetcher = UserHistoryFetcher()
    results = fetcher.get_multiple_users_history(
        wallet_addresses=wallet_addresses,
        max_workers=MAX_WORKERS,
        delay_between_requests=DELAY_BETWEEN_REQUESTS
    )
    
    # Save results
    fetcher.save_to_json(results, OUTPUT_FILE)
    
    # Print summary
    print("\n" + "-" * 40)
    print("SUMMARY")
    print("-" * 40)
    total_positions = sum(r["total_positions"] for r in results if r["status"] == "success")
    print(f"Total wallets processed: {len(results)}")
    print(f"Total positions found: {total_positions}")
    print(f"Successful: {sum(1 for r in results if r['status'] == 'success')}")
    print(f"Failed: {sum(1 for r in results if r['status'] == 'failed')}")


if __name__ == "__main__":
    main()