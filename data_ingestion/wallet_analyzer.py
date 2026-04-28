# Combines profile enrichment and trading history
# Generates a single JSON file with complete information per wallet
# Profile data & trading history

import requests
import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


class WalletAnalyzer:
    
    def __init__(self):
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.profile_url = "https://polymarket.com/api/profile/userData"
        self.history_url = "https://data-api.polymarket.com/closed-positions"
    
    def fetch_profile(self, wallet_address: str) -> Dict[str, Any]:
        """Fetch public profile for a wallet."""
        params = {"address": wallet_address}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": f"https://polymarket.com/profile/{wallet_address}"
        }
        
        try:
            response = requests.get(self.profile_url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "name": data.get("name"),
                    "profileImage": data.get("profileImage"),
                    "createdAt": data.get("createdAt"),
                    "bio": data.get("bio"),
                    "pseudonym": data.get("pseudonym"),
                    "verifiedBadge": data.get("verifiedBadge", False),
                    "userId": data.get("id")
                }
        except Exception:
            pass
        
        return {"name": None, "profileImage": None, "createdAt": None}
    
    def fetch_history(self, wallet_address: str) -> Dict[str, Any]:
        # Fetch all closed positions for a wallet using pagination
        all_positions = []
        offset = 0
        page = 1
        limit_per_page = 100
        
        # Pagination loop: fetch all positions, not just first page
        while True:
            params = {
                "user": wallet_address,
                "limit": limit_per_page,
                "offset": offset,
                "sortBy": "REALIZEDPNL",
                "sortDirection": "DESC"
            }
            
            try:
                response = requests.get(self.history_url, params=params, timeout=10)
                
                if response.status_code != 200:
                    break
                
                data = response.json()
                if not data:
                    break
                
                all_positions.extend(data)
                
                # If we got fewer than limit, this is the last page
                if len(data) < limit_per_page:
                    break
                
                offset += limit_per_page
                page += 1
                
            except Exception:
                break
        
        if not all_positions:
            return {"total_positions": 0, "summary": {"total_won": 0, "total_lost": 0, "net_pnl": 0}, "positions": []}
        
        # Process positions into cleaner format
        processed = []
        total_won = 0
        total_lost = 0
        
        for pos in all_positions:
            realized_pnl = float(pos.get("realizedPnl", 0))
            
            # Determine status based on PnL
            if realized_pnl > 0:
                total_won += realized_pnl
                status = "WON"
            elif realized_pnl < 0:
                total_lost += realized_pnl
                status = "LOST"
            else:
                status = "TIE"
            
            processed.append({
                "market_title": pos.get("title", "Unknown"),
                "outcome": pos.get("outcome", "N/A"),
                "realized_pnl": round(realized_pnl, 2),
                "status": status
            })
        
        return {
            "total_positions": len(processed),
            "summary": {
                "total_won": round(total_won, 2),
                "total_lost": round(abs(total_lost), 2),
                "net_pnl": round(total_won + total_lost, 2)
            },
            "positions": processed # Store all positions without any limit
        }
    
    def analyze_wallet(self, wallet_address: str) -> Dict[str, Any]:
        # Check cache first
        if wallet_address in self.cache:
            return self.cache[wallet_address]
        
        print(f"  Analyzing: {wallet_address[:8]}...")
        
        profile = self.fetch_profile(wallet_address)
        history = self.fetch_history(wallet_address)
        
        result = {
            "wallet_address": wallet_address,
            "profile": profile,
            "trading": history
        }
        
        self.cache[wallet_address] = result
        
        # Print progress
        if profile.get("name"):
            print(f"    ✓ {profile['name']} - {history['total_positions']} positions, net: ${history['summary']['net_pnl']}")
        else:
            print(f"    ✓ Anonymous - {history['total_positions']} positions, net: ${history['summary']['net_pnl']}")
        
        return result
    
    def analyze_multiple_wallets(
        self,
        wallet_addresses: List[str],
        max_workers: int = 5
    ) -> List[Dict[str, Any]]:
        # Fetch history for multiple wallets in parallel
        results = []
        success_count = 0
        failed_count = 0
        
        print(f"\n{'='*60}")
        print(f"ANALYZING {len(wallet_addresses)} WALLETS")
        print(f"{'='*60}\n")
        
        def analyze(wallet):
            return self.analyze_wallet(wallet)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(analyze, wallet): wallet for wallet in wallet_addresses}
            
            for idx, future in enumerate(as_completed(futures), 1):
                try:
                    result = future.result(timeout=60)
                    results.append(result)
                    success_count += 1
                except Exception as e:
                    wallet = futures[future]
                    failed_count += 1
                    print(f"[{idx}/{len(wallet_addresses)}] ✗ {wallet[:8]}... - error: {e}")
                    results.append({
                        "wallet_address": wallet,
                        "profile": {"name": None},
                        "trading": {"error": str(e)}
                    })
                
                print(f"[{idx}/{len(wallet_addresses)}] Completed")
                time.sleep(0.1)
        
        return results
    
    def save_results(self, results: List[Dict[str, Any]], output_path: str) -> bool:
        # Save combined results to JSON file
        try:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            output_data = {
                "metadata": {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "total_wallets": len(results),
                    "profiles_found": sum(1 for r in results if r["profile"].get("name")),
                    "successful_fetches": len(results)
                },
                "wallets": results
            }
            
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            print(f"\n[✓] Results saved to: {output_path}")
            return True
            
        except Exception as e:
            print(f"[X] Error saving: {e}")
            return False


def load_wallets_from_file(file_path: str = "data_ingestion/unique_wallets_list.json") -> List[str]:
    # Load wallet addresses from unique_wallets_list.json
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        wallets = data.get("wallet_addresses", [])
        print(f"Loaded {len(wallets)} wallets from {file_path}")
        return wallets
        
    except Exception as e:
        print(f"[X] Error loading wallets: {e}")
        return []


def main():
    INPUT_FILE = "data_ingestion/unique_wallets_list.json"
    OUTPUT_FILE = "data_ingestion/wallets_complete_data.json"
    MAX_WORKERS = 5  # Good balance between speed and safety (due to rate limits)
    # Velue can be changed
    
    print("=" * 60)
    print("WALLET ANALYZER")
    print("Fetches profile + trading history for each wallet")
    print("=" * 60)
    
    # Load wallets from unique list
    wallet_addresses = load_wallets_from_file(INPUT_FILE)
    
    if not wallet_addresses:
        print("No wallets found. Run top_wallets_processor.py first.")
        return
    
    # Analyze all wallets
    analyzer = WalletAnalyzer()
    results = analyzer.analyze_multiple_wallets(wallet_addresses, max_workers=MAX_WORKERS)
    
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


if __name__ == "__main__":
    main()