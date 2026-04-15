import requests
import json
from datetime import datetime, timezone

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
    params = {"limit": limit, "offset": 0}
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        return {}
    
    trades = response.json().get("response", [])

    from collections import defaultdict
    price_map = defaultdict(lambda: {"wallets": {}, "trade_count": 0})

    for trade in trades:
        price = trade.get("price")
        address = trade.get("address")
        username = trade.get("username", "")
        amount = float(trade.get("amount", 0))
        side = trade.get("side")  # True = BUY

        if price and address and side:  # solo bids
            price_map[price]["trade_count"] += 1
            if address not in price_map[price]["wallets"]:
                price_map[price]["wallets"][address] = {
                    "total_amount": 0.0,
                    "username": username
                }
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
    file_name = "top_wallets_data.json"
    with open(file_name, "w", encoding="utf-8") as file:
        json.dump(all_wallets, file, indent=4, ensure_ascii=False)
    print(f"Top wallets data saved to '{file_name}'")