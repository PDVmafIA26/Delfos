import requests
import time
import json

def get_markets_info(tag_slug, generate_json=False):
    """
    Fetches event and market data from the Polymarket Gamma API for a specific tag.
    Extracts active markets, returns a mapping of market IDs to their CLOB token IDs,
    and optionally saves the filtered, raw API response to a JSON file.

    Args:
        tag_slug (str): The category tag to query (e.g., 'tech', 'politics').
        generate_json (bool): If True, saves the filtered API response to a JSON file.

    Returns:
        dict: A mapping where keys are market IDs (str) and values are lists of CLOB token IDs (list of str).
    """
    # API Endpoint for paginated events
    url_events = "https://gamma-api.polymarket.com/events/keyset"
    current_cursor = None

    # Dictionary to store the lightweight mapping required for WebSocket subscriptions
    market_tokens_mapping = {}

    # List to accumulate the raw events from the API (filtered for active markets only)
    filtered_api_events = []

    page = 1
    print(f"--- Starting data extraction for tag: '{tag_slug}' | Generate JSON: {generate_json} ---")

    while True:
        # Define query parameters
        params = {
            "tag_slug": tag_slug,
            "limit": 100,
            "closed": "false"  # Instructs the API to fetch mostly open events
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
                active_markets_in_event = []

                for market in event.get("markets", []):
                    # Strict check to ensure the market is still active
                    if market.get("closed") is False:
                        active_markets_in_event.append(market)

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

                # If the event contains at least one active market, keep it for the JSON dump
                if active_markets_in_event:
                    # Overwrite the 'markets' list with our strictly filtered active markets
                    event["markets"] = active_markets_in_event
                    filtered_api_events.append(event)

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

    print(f"\n[✓] Extraction complete. Total active markets extracted: {len(market_tokens_mapping)}")

    # Perform a direct JSON dump of the filtered API payload if requested
    if generate_json:
        filename = f"polymarket_{tag_slug}.json"
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(filtered_api_events, file, indent=4, ensure_ascii=False)

        print(f"[✓] Filtered API payload successfully saved to: {filename}")

    return market_tokens_mapping

# ==========================================
# MAIN EXECUTION BLOCK
# ==========================================
if __name__ == "__main__":
    CATEGORIES_TAG = ["politics", "geopolitics", "tech", "finance", "economy"]

    for category in CATEGORIES_TAG:
        get_markets_info(category, generate_json=True)