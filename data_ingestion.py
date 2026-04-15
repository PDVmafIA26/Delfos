import data_ingestion.markets as mar
import requests
import json
import time
from datetime import datetime, timezone
from data_ingestion.top_wallets_processor import *


def get_order_book(token_id):
    """
    Fetches the Central Limit Order Book (CLOB) for a specific token.
    Retrieves all pending Bids (buy orders) and Asks (sell orders).
    
    Args:
        token_id (str): The specific token identifier for the outcome (e.g., 'Yes' token).
        
    Returns:
        dict: A dictionary containing 'bids' and 'asks' lists.
    """
    url = "https://clob.polymarket.com/book"
    params = {"token_id": token_id}
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"  [X] Failed fetching order book for token {token_id}. Status: {response.status_code}")
        return {"bids": [], "asks": []}


def run_advanced_ingestion(category_tag, keywords):
    """
    Main data engineering pipeline. 
    It filters relevant markets, extracts deep metadata, fetches the order book, 
    and consolidates the data into a Golden Record payload for the Bronze Layer.
    
    Args:
        category_tag (str): The Polymarket category slug (e.g., 'tech').
        keywords (list): Keywords indicating asymmetric information events.
        
    Returns:
        list: A list of enriched market dictionaries ready for ingestion.
    """
    print("=== PHASE 1: Identifying target markets ===")
    
    # Retrieve filtered market questions using the external module
    interesting_questions = mar.get_interesting_markets(category_tag, keywords)
    questions_set = set(interesting_questions)
    
    if not questions_set:
        print("[X] No target markets found. Aborting pipeline.")
        return []

    print("\n=== PHASE 2: Scanning Keysets for Market IDs ===")
    tag_id = mar.get_id_by_slug(category_tag)
    url_keyset = "https://gamma-api.polymarket.com/markets/keyset"
    current_cursor = None 
    target_market_ids = []
    
    # Fast scan through the paginated endpoint to map questions to their IDs
    while True:
        params = {"tag_id": tag_id, "limit": 100, "active": "true", "closed": "false"}
        if current_cursor: 
            params["after_cursor"] = current_cursor
            
        response = requests.get(url_keyset, params=params)
        if response.status_code != 200: 
            print(f"[X] Keyset API failure: {response.status_code}")
            break
        
        data = response.json()
        for market in data.get("markets", []):
            if market.get('question') in questions_set:
                target_market_ids.append(market.get("id"))
                
        current_cursor = data.get("next_cursor")
        if not current_cursor or current_cursor == "LTE=": 
            break

    print(f"[✓] Found {len(target_market_ids)} market IDs for deep-scanning.")

    print("\n=== PHASE 3: Deep Scan & Order Book Extraction ===")
    enriched_payloads = []
    
    for i, m_id in enumerate(target_market_ids, 1):
        print(f"  -> Scanning {i}/{len(target_market_ids)} (Market ID: {m_id})...")
        
        # 1. Fetch Deep Metadata
        deep_data = get_deep_market_info(m_id)
        if not deep_data: 
            continue
            
        # 2. Extract the 'Yes' Token ID securely
        raw_clob_tokens = deep_data.get("clobTokenIds", [])
        
        # Handle API inconsistency where tokens might be returned as a stringified JSON
        if isinstance(raw_clob_tokens, str):
            try:
                clob_tokens = json.loads(raw_clob_tokens)
            except json.JSONDecodeError:
                clob_tokens = []
        else:
            clob_tokens = raw_clob_tokens
            
        # The index 0 typically represents the 'Yes' token in binary markets
        yes_token_id = clob_tokens[0] if clob_tokens and len(clob_tokens) > 0 else None
        
        # 3. Fetch Order Book via CLOB API
        order_book = get_order_book(yes_token_id) if yes_token_id else {"bids": [], "asks": []}
        wallets_by_price = get_top_wallets_by_price_synthesis(
            deep_data.get("conditionId", "")
        )
        # 4. Construct the Golden Payload for downstream processing
        golden_record = {
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "market_id": deep_data.get("id"),
            "event_id": deep_data.get("eventId"),          
            "question": deep_data.get("question"),
            "description": deep_data.get("description"),   
            "oracle": deep_data.get("oracle"),             
            "outcomes": deep_data.get("outcomes"),
            "outcome_prices": deep_data.get("outcomePrices"),
            "volume": deep_data.get("volume"),
            "liquidity": deep_data.get("liquidity"),
            "yes_token_id": yes_token_id,
            "order_book": {                                
                "bids": order_book.get("bids", [])[:10],   # Keep only the top 10 buy walls to reduce payload size
                "asks": order_book.get("asks", [])[:10]    # Keep only the top 10 sell walls to reduce payload size
            },
            "wallets_per_bid_price": {
                price: data
                for price, data in wallets_by_price.items()
                if float(price) < 0.5   # solo bids
            }
        }
        
        enriched_payloads.append(golden_record)
        
        # Rate limiting delay to respect API boundaries
        time.sleep(0.2) 

    return enriched_payloads


# ==========================================
# MAIN EXECUTION BLOCK
# ==========================================
if __name__ == "__main__":
    CATEGORY = "tech"
    
    # Target keywords for detecting asymmetric information events (M&A, bankruptcies, etc.)
    KEYWORDS = [
        "bankruptcy", "acquire", "merger", "arrested", "jail", "charged",
        "out as", "next ceo", "incident", "disrupted", "outage", 
        "take a stake", "lawsuit", "sues", "banned"
    ]

    final_data = run_advanced_ingestion(CATEGORY, KEYWORDS)
    
    print(f"\n[✓] PIPELINE COMPLETE. Generated {len(final_data)} Enriched JSON records.")
    
    if final_data:
        file_name = f"bronze_layer_payload_{CATEGORY}.json"
        
        with open(file_name, "w", encoding="utf-8") as file:
            json.dump(final_data, file, indent=4, ensure_ascii=False)
            
        print(f"[✓] Data successfully saved to '{file_name}'.")
        
        # Automatically run top wallets ingestion with the market_ids from the pipeline
        market_ids = [record["market_id"] for record in final_data]
        print(f"\n[→] Starting top wallets ingestion for {len(market_ids)} markets...")
        run_top_wallets_ingestion(market_ids)
