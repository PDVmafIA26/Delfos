import requests
import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Set
from collections import defaultdict
from pathlib import Path

def get_deep_market_info(market_id):
    """
    Fetches the detailed metadata to get conditionId.
    """
    url = f"https://gamma-api.polymarket.com/markets/{market_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None

def get_top_wallets_by_price_synthesis(condition_id, limit=500):
    """
    Usa Synthesis API para obtener wallets por precio.
    """
    url = f"https://synthesis.trade/api/v1/polymarket/market/{condition_id}/trades"
    
    all_trades = []
    offset = 0
    max_pages = 5  # Limit to 5 pages maximum to prevent infinite loops

    for page in range(max_pages):
        params = {"limit": limit, "offset": offset}
        try:
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                break
            
            trades = response.json().get("response", [])
            if not trades:
                break
            
            all_trades.extend(trades)

            # If we receive fewer trades than requested, it's the last page
            if len(trades) < limit:
                break

            offset += limit

        except Exception as e:
            print(f"  [WARN] Error fetching trades: {e}")
            break
    
    trades = all_trades

    if not trades:
        return {}

    from collections import defaultdict
    price_map = defaultdict(lambda: {"wallets": {}, "trade_count": 0})

    for trade in trades:
        price = trade.get("price")
        address = trade.get("address")
        username = trade.get("username", "")
        profile_image_url = trade.get("image") or trade.get("profile_image") or trade.get("profile_image_url") or trade.get("avatar") or trade.get("avatar_url") or ""
        amount = float(trade.get("amount", 0))
        side = trade.get("side")  # True = BUY

        if price and address and side:  # solo bids
            price_map[price]["trade_count"] += 1
            if address not in price_map[price]["wallets"]:
                price_map[price]["wallets"][address] = {
                    "total_amount": 0.0,
                    "username": username,
                    "profile_image_url": profile_image_url
                }
            else:
                # Mantener la primera imagen capturada si ya existe para la cartera
                if not price_map[price]["wallets"][address].get("profile_image_url"):
                    price_map[price]["wallets"][address]["profile_image_url"] = profile_image_url
            price_map[price]["wallets"][address]["total_amount"] += amount

    result = {}
    for price, data in sorted(price_map.items(), key=lambda x: float(x[0])):
        top_wallets = sorted(
            data["wallets"].items(),
            key=lambda x: x[1]["total_amount"],
            reverse=True
        )[:5]
        result[price] = {
            "unique_accounts": len(data["wallets"]),
            "trade_count": data["trade_count"],
            "top_wallets": [
                {
                    "address": addr,
                    "username": info["username"],
                    "profile_image_url": info.get("profile_image_url", ""),
                    "total_amount_usd": round(info["total_amount"], 2)
                }
                for addr, info in top_wallets
            ]
        }

    return result

def run_top_wallets_ingestion(market_ids):
    """
    Run ingestion for top wallets of given markets.
    Saves to JSON file.
    """
    all_wallets = {}
    for market_id in market_ids:
        deep_data = get_deep_market_info(market_id)
        if not deep_data:
            continue
        condition_id = deep_data.get("conditionId", "")
        wallets = get_top_wallets_by_price_synthesis(condition_id)
        all_wallets[market_id] = {
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "wallets_per_bid_price": wallets
        }
        print(f"Processed top wallets for market {market_id}")

    # Save to JSON file
    file_name = "data_ingestion/top_wallets_data.json"
    with open(file_name, "w", encoding="utf-8") as file:
        json.dump(all_wallets, file, indent=4, ensure_ascii=False)
    print(f"Top wallets data saved to '{file_name}'")

     # Prevent duplicates
    unique_wallets = set()
    
    # Iterate through each market's data
    for market_id, market_data in all_wallets.items():
        wallets_per_price = market_data.get("wallets_per_bid_price", {})
        
        # Iterate through each price level within the market
        for price_level, price_data in wallets_per_price.items():
            top_wallets = price_data.get("top_wallets", [])
            
            # Extract wallet address from each top wallet entry
            for wallet in top_wallets:
                address = wallet.get("address")
                # Only add valid Ethereum addresses (starting with 0x)
                if address and address.startswith("0x"):
                    unique_wallets.add(address)
    
    # Save unique wallets list to JSON file for later enrichment
    unique_file_name = "data_ingestion/unique_wallets_list.json"
    unique_data = {
        "generated_at": datetime.now(timezone.utc).isoformat(), # UTC timestamp for reproducibility
        "total_unique_wallets": len(unique_wallets), # Count of unique addresses found
        "wallet_addresses": list(unique_wallets) # Convert set to list for JSON serialization
    }
    
    # Write the unique wallets data to disk
    with open(unique_file_name, "w", encoding="utf-8") as f:
        json.dump(unique_data, f, indent=2, ensure_ascii=False)
    
    # Output confirmation messages for logging/debugging
    print(f"Unique wallets saved to '{unique_file_name}'")
    print(f"{len(unique_wallets)} unique wallets extracted")


