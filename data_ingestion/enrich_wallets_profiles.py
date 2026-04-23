import requests
import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


class WalletProfileEnricher:
    
    def __init__(self):
        self.cache: Dict[str, Optional[Dict[str, Any]]] = {}
    
    def fetch_profile(self, wallet_address: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        if wallet_address in self.cache:
            return self.cache[wallet_address]
        
        url = "https://polymarket.com/api/profile/userData"
        params = {"address": wallet_address}
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": f"https://polymarket.com/profile/{wallet_address}"
        }
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    result = {
                        "wallet_address": wallet_address,
                        "name": data.get("name"),
                        "profileImage": data.get("profileImage"),
                        "createdAt": data.get("createdAt"),
                        "bio": data.get("bio"),
                        "pseudonym": data.get("pseudonym"),
                        "verifiedBadge": data.get("verifiedBadge", False),
                        "userId": data.get("id")
                    }
                    self.cache[wallet_address] = result
                    if result.get("name"):
                        print(f"  ✓ Found: {result['name']}")
                    return result
                    
                elif response.status_code == 404:
                    break
                    
            except Exception as e:
                print(f"  Error (attempt {attempt+1}): {e}")
            
            time.sleep(1)
        
        result = {
            "wallet_address": wallet_address,
            "name": None,
            "profileImage": None,
            "createdAt": None,
            "bio": None,
            "pseudonym": None,
            "verifiedBadge": None,
            "note": "No profile found"
        }
        self.cache[wallet_address] = result
        return result
    
    def enrich_from_unique_wallets_file(
        self,
        input_path: str = "data_ingestion/unique_wallets_list.json",
        output_path: str = "data_ingestion/enriched_wallets_profiles.json",
        max_workers: int = 3,
        delay_between_requests: float = 0.2
    ) -> bool:
        try:
            input_file = Path(input_path)
            if not input_file.exists():
                print(f"[X] Input file not found: {input_path}")
                print("    Please run top_wallets_processor.py first.")
                return False
                
            with open(input_file, 'r', encoding='utf-8') as f:
                wallet_data = json.load(f)
                
        except Exception as e:
            print(f"[X] Error reading input file: {e}")
            return False
        
        wallet_addresses = wallet_data.get("wallet_addresses", [])
        
        if not wallet_addresses:
            print("[X] No wallet addresses found in the input file.")
            return False
        
        print("=" * 60)
        print("WALLET PROFILE ENRICHER")
        print(f"Found {len(wallet_addresses)} unique wallets to enrich")
        print("=" * 60)
        
        enriched_wallets = []
        success_count = 0
        failed_count = 0
        
        print("\nUsing sequential mode...")
        for idx, wallet in enumerate(wallet_addresses, 1):
            print(f"[{idx}/{len(wallet_addresses)}] Enriching: {wallet[:8]}...")
            
            profile = self.fetch_profile(wallet)
            enriched_wallets.append(profile)
            
            if profile and profile.get("name"):
                success_count += 1
            else:
                failed_count += 1
            
            if idx < len(wallet_addresses):
                time.sleep(delay_between_requests)
        
        output_data = {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "source_file": str(input_path),
                "source_generated_at": wallet_data.get("generated_at"),
                "total_wallets_requested": len(wallet_addresses),
                "successful_fetches": success_count,
                "failed_fetches": failed_count
            },
            "wallets": enriched_wallets
        }
        
        try:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
                
            print(f"\n[✓] Enriched data saved to: {output_path}")
            print(f"    - Wallets processed: {len(wallet_addresses)}")
            print(f"    - Profiles found: {success_count}")
            print(f"    - Failed/Not found: {failed_count}")
            
            self._print_summary(enriched_wallets)
            
            return True
            
        except Exception as e:
            print(f"[X] Error saving output file: {e}")
            return False
    
    def _print_summary(self, enriched_wallets: List[Dict[str, Any]]) -> None:
        print("\n" + "-" * 40)
        print("ENRICHMENT SUMMARY")
        print("-" * 40)
        
        wallets_with_names = 0
        wallets_with_images = 0
        wallets_with_bios = 0
        
        for wallet in enriched_wallets:
            if wallet and wallet.get("name"):
                wallets_with_names += 1
            if wallet and wallet.get("profileImage"):
                wallets_with_images += 1
            if wallet and wallet.get("bio"):
                wallets_with_bios += 1
        
        print(f"Wallets with display names: {wallets_with_names}/{len(enriched_wallets)}")
        print(f"Wallets with profile images: {wallets_with_images}/{len(enriched_wallets)}")
        print(f"Wallets with bios: {wallets_with_bios}/{len(enriched_wallets)}")


def main():
    INPUT_FILE = "data_ingestion/unique_wallets_list.json"
    OUTPUT_FILE = "data_ingestion/enriched_wallets_profiles.json"
    
    print("=" * 60)
    print("WALLET PROFILE ENRICHER")
    print("Adding public profile data to top wallets")
    print("=" * 60)
    
    enricher = WalletProfileEnricher()
    
    success = enricher.enrich_from_unique_wallets_file(
        input_path=INPUT_FILE,
        output_path=OUTPUT_FILE
    )
    
    if success:
        print("\n[✓] Enrichment complete!")
        print(f"    Output file: {OUTPUT_FILE}")
    else:
        print("\n[X] Enrichment failed. Check errors above.")


if __name__ == "__main__":
    main()