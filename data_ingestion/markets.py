import requests
import time
import json

def get_polymarket_data(tag_slug, generate_json=False):
    """
    Fetches event and market data from the Polymarket Gamma API based on a specific tag.
    Extracts active market IDs and optionally saves a cleaned JSON file of the events.
    
    Args:
        tag_slug (str): The category tag to query (e.g., 'tech', 'politics').
        generate_json (bool): If True, saves a formatted JSON file locally.
        
    Returns:
        list: A list of unique, active market IDs.
    """
    url_events = "https://gamma-api.polymarket.com/events/keyset"
    current_cursor = None 
    
    total_market_ids = []
    total_cleaned_events = []
    page = 1
    
    print(f"--- Starting extraction for: {tag_slug} | Generate JSON: {generate_json} ---")
    
    while True:
        # Define the API query parameters
        params = {
            "tag_slug": tag_slug,
            "limit": 100,
            "closed": "false" # Fetch only open events
        }
        
        # Append pagination cursor if it exists from the previous iteration
        if current_cursor:
            params["after_cursor"] = current_cursor
            
        try:
            response = requests.get(url_events, params=params, timeout=10)
            response.raise_for_status() # Raise an exception for HTTP errors
            
            data = response.json()
            events = data.get("events", [])
            
            for event in events:
                # Create a shallow copy of the event to preserve original metadata
                cleaned_event = event.copy()
                event_market_ids = []
                
                # Iterate through the markets associated with the current event
                for market in event.get("markets", []):
                    # Filter out markets that are already closed
                    if market.get("closed") is False:
                        market_id = market.get("id")
                        if market_id:
                            event_market_ids.append(market_id)
                            total_market_ids.append(market_id)
                
                # REPLACE the complex market objects with a simple list of market IDs
                cleaned_event["markets"] = event_market_ids
                total_cleaned_events.append(cleaned_event)
            
            print(f"Page {page}: processed {len(events)} events.")

            # Check for the next pagination cursor
            current_cursor = data.get("next_cursor")
            
            # 'LTE=' or None signifies the end of the API dataset
            if not current_cursor or current_cursor == "LTE=":
                break
                
            page += 1
            # Rate limiting courtesy pause to prevent getting blocked by the API
            time.sleep(0.2) 
            
        except requests.exceptions.RequestException as e:
            print(f"\n[X] Critical request error: {e}")
            break
            
    # Remove duplicates from the global market IDs list while preserving order
    unique_market_ids = list(dict.fromkeys(total_market_ids))
    print(f"\n[✓] Total: {len(unique_market_ids)} unique active Market IDs extracted.")
    
    if generate_json:
        # Remove duplicate events based on their unique 'id'
        unique_events_dict = {evt.get("id"): evt for evt in total_cleaned_events if evt.get("id")}
        unique_events = list(unique_events_dict.values())
        
        # 1. CREATE THE FINAL JSON STRUCTURE
        json_structure = {
            "category": tag_slug,
            "events": unique_events
        }
        
        # 2. CONVERT TO JSON STRING (with indentation for readability)
        formatted_json = json.dumps(json_structure, indent=4)
        
        # 3. SAVE IT TO A LOCAL FILE
        filename = f"polymarket_{tag_slug}.json"
        with open(filename, "w", encoding="utf-8") as file:
            file.write(formatted_json)
            
        print(f"[✓] JSON file successfully generated and saved as: {filename}")
        
        return unique_market_ids
    
    else:
        # If generate_json is False, we simply return the list of IDs
        return unique_market_ids
    
    
# ==========================================
# MAIN EXECUTION BLOCK
# ==========================================
if __name__ == "__main__":
    CATEGORIES_TAG = ["politics", "geopolitics", "tech", "finance", "economy"]

    for category in CATEGORIES_TAG:
        get_polymarket_data(category, generate_json=True)