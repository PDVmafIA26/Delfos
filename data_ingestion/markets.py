import requests
import time
import json

def get_id_by_slug(tag_slug):
    """
    Communicates with the Polymarket API to convert a category slug (e.g., 'tech') 
    into its corresponding numeric ID.
    
    Args:
        tag_slug (str): The category identifier (e.g., 'tech', 'crypto', 'politics').
        
    Returns:
        int or None: The numeric ID of the tag, or None if the request fails.
    """
    url_tag = f"https://gamma-api.polymarket.com/tags/slug/{tag_slug}"
    print(f"Fetching numeric ID for category '{tag_slug}'...")
    
    response_tag = requests.get(url_tag)
    if response_tag.status_code != 200:
        print(f"[X] Error: Could not retrieve ID for tag '{tag_slug}'. Status code: {response_tag.status_code}")
        return None
        
    tag_data = response_tag.json()
    tag_id = tag_data.get("id")
    print(f"[✓] Numeric ID found: {tag_id}\n")
    
    return tag_id

def get_tag_ids_by_slug_list(category_tags):
    """
    Iterates through a list of category slugs and retrieves their numeric IDs.
    
    Args:
        category_tags (list): A list of category slugs (strings).
        
    Returns:
        dict: A mapping of category slugs to their numeric IDs.
    """
    categories = {}

    for category in category_tags:
        categories[category] = get_id_by_slug(category)
    
    return categories
    
def get_markets_info(tag_slug, ids_categories_exclude, generate_json=False):
    """
    Fetches event and market data from the Polymarket Gamma API for a specific tag.
    Extracts all markets, returns a mapping of market IDs to their CLOB token IDs,
    and optionally returns the raw API response (JSON) without saving it to a file.

    Args:
        tag_slug (str): The category tag to query (e.g., 'tech', 'politics').
        ids_categories_exclude (list): List of tag IDs to exclude.
        generate_json (bool): If True, also returns the raw API response as a list of dictionaries.

    Returns:
        If generate_json is False:
            dict: A mapping where keys are market IDs (str) and values are lists of CLOB token IDs (list of str).
        If generate_json is True:
            tuple: (dict of market_tokens_mapping, list of all raw events from the API)
    """
    # API Endpoint for paginated events
    url_events = "https://gamma-api.polymarket.com/events/keyset"
    current_cursor = None

    # Dictionary to store the lightweight mapping required for WebSocket subscriptions
    market_tokens_mapping = {}

    # List to accumulate the raw events exactly as they come from the API
    all_api_events = []

    page = 1
    print(f"--- Starting data extraction for tag: '{tag_slug}' | Return JSON: {generate_json} ---")

    while True:
        # Define query parameters (removed "closed": "false" to fetch everything)
        params = {
            "tag_slug": tag_slug,
            "limit": 100,
            "exclude_tag_id": ids_categories_exclude
        }

        # Append pagination cursor if available from the previous iteration
        if current_cursor:
            params["after_cursor"] = current_cursor

        try:
            response = requests.get(url_events, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()
            events = data.get("events", [])

            for event in events:
                for market in event.get("markets", []):
                    market_id = market.get("id")
                    if market_id:
                        # Safely parse clobTokenIds, handling both stringified JSON and native lists
                        raw_tokens = market.get("clobTokenIds", "[]")
                        parsed_tokens = []

                        if isinstance(raw_tokens, str):
                            try:
                                parsed_tokens = json.loads(raw_tokens)
                            except json.JSONDecodeError:
                                pass  # Keep it as an empty list if decoding fails
                        elif isinstance(raw_tokens, list):
                            parsed_tokens = raw_tokens

                        # Populate the mapping for the WebSocket payload
                        market_tokens_mapping[market_id] = parsed_tokens

            # Accumulate raw API payload if requested
            if generate_json:
                all_api_events.extend(events)

            print(f"Page {page} processed: {len(events)} events evaluated.")

            # Retrieve the cursor for the next page
            current_cursor = data.get("next_cursor")

            # A missing cursor or "LTE=" indicates the end of the dataset
            if not current_cursor or current_cursor == "LTE=":
                break

            page += 1
            # Courtesy delay to respect API rate limits
            time.sleep(0.2)

        except requests.exceptions.RequestException as e:
            print(f"\n[X] Critical HTTP request error: {e}")
            break

    print(f"\n[✓] Extraction complete. Total markets extracted: {len(market_tokens_mapping)}")

    if generate_json:
        return market_tokens_mapping, all_api_events
    
    return market_tokens_mapping

# ==========================================
# MAIN EXECUTION BLOCK
# ==========================================
if __name__ == "__main__":
    CATEGORIES_TAG = ["politics", "geopolitics", "tech", "finance", "economy"]
    categories = get_tag_ids_by_slug_list(CATEGORIES_TAG)
    
    # This file should ideally only be generated during the first execution.
    # For subsequent runs, the JSON should be loaded as a dictionary to save API requests.
    # Therefore, this file should be stored in a persistent Docker volume.
    json_string = json.dumps(categories, indent=2)

    ids_categories_exclude = []
    
    # List to accumulate ALL events across all categories
    all_collected_events = []
    # Dictionary to accumulate all market token mappings
    total_market_mapping = {}

    for category in CATEGORIES_TAG:
        # Call the method requesting it to return the JSON as well (generate_json=True)
        mapping, events_json = get_markets_info(category, ids_categories_exclude, generate_json=True)
        
        # Accumulate the results into our global execution variables
        total_market_mapping.update(mapping)
        all_collected_events.extend(events_json)

        ids_categories_exclude.append(categories[category])
    
    output_filename = "polymarket_all_categories.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(all_collected_events, f, indent=4, ensure_ascii=False)

    print("\n[✓] Process completed.")
    print(f"    - Total markets mapped: {len(total_market_mapping)}")
    print(f"    - Total events saved to '{output_filename}': {len(all_collected_events)}")