import requests
from datetime import datetime, timezone
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# File name for caching category tag IDs
TAGS_FILE = "categories_tags.json"

def get_id_by_slug(session, tag_slug):
    """
    Retrieves the numeric ID for a given category slug from the Polymarket Gamma API.
    
    Args:
        session (requests.Session): The HTTP session object.
        tag_slug (str): The string identifier for the category (e.g., 'politics').
        
    Returns:
        int or None: The numeric ID of the tag, or None if the request fails.
    """
    url_tag = f"https://gamma-api.polymarket.com/tags/slug/{tag_slug}"
    print(f"Fetching numeric ID for category '{tag_slug}'...")
    
    response_tag = session.get(url_tag)
    if response_tag.status_code != 200:
        print(f"[X] Error: Could not retrieve ID for tag '{tag_slug}'. Status code: {response_tag.status_code}")
        return None
        
    tag_data = response_tag.json()
    tag_id = tag_data.get("id")
    print(f"[✓] Numeric ID found: {tag_id}\n")
    return tag_id

def get_tag_ids_by_slug_list(session, category_tags):
    """
    Iterates through a list of category slugs and maps them to their numeric IDs.
    """
    categories = {}
    for category in category_tags:
        categories[category] = get_id_by_slug(session, category)
    return categories

def get_order_book(session, token_id):
    """
    Fetches the Central Limit Order Book (CLOB) data for a specific token.
    
    Returns:
        tuple: (token_id, order_book_data) to easily map the data back to the 
               correct market after concurrent processing.
    """
    url = "https://clob.polymarket.com/book"
    params = {"token_id": token_id}
    
    try:
        response = session.get(url, params=params, timeout=5)
        if response.status_code == 200:
            return token_id, response.json()
        else:
            # Fallback to empty structures if the order book is unavailable
            return token_id, {"bids": [], "asks": []}
    except requests.exceptions.RequestException:
        return token_id, {"bids": [], "asks": []}

def get_markets_info(session, tag_slug, ids_categories_exclude, generate_json=False):
    """
    Fetches event and market data from the Polymarket Gamma API using pagination,
    then concurrently fetches the associated order books for open markets.
    """
    url_events = "https://gamma-api.polymarket.com/events/keyset"
    current_cursor = None

    market_tokens_mapping = {}
    all_api_events = []
    
    # Use a set to store unique tokens and avoid redundant API calls
    tokens_to_fetch = set() 

    page = 1
    print(f"--- Starting data extraction for tag: '{tag_slug}' ---")

    # PHASE 1: Paginate through events and collect tokens that require Order Books
    while True:
        params = {
            "tag_slug": tag_slug,
            "limit": 100,
            "exclude_tag_id": ids_categories_exclude,
            "closed": "false" 
        }
        if current_cursor:
            params["after_cursor"] = current_cursor

        try:
            response = session.get(url_events, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            events = data.get("events", [])

            for event in events:
                for market in event.get("markets", []):
                    market_id = market.get("conditionId")
                    is_closed = market.get("closed", False)
                    
                    if market_id:
                        raw_tokens = market.get("clobTokenIds", "[]")
                        parsed_tokens = []

                        # Safely parse the token IDs regardless of their data type
                        if isinstance(raw_tokens, str):
                            try:
                                parsed_tokens = json.loads(raw_tokens)
                            except json.JSONDecodeError:
                                pass
                        elif isinstance(raw_tokens, list):
                            parsed_tokens = raw_tokens

                        # Overwrite with the cleanly parsed list for easier downstream processing
                        market["clobTokenIds"] = parsed_tokens
                        market_tokens_mapping[market_id] = parsed_tokens
                        market["order_books"] = {}

                        if parsed_tokens:
                            if not is_closed:
                                # Add tokens to the parallel download queue
                                tokens_to_fetch.update(parsed_tokens)
                            else:
                                # Inject empty order books for closed markets to maintain schema consistency
                                for token in parsed_tokens:
                                    market["order_books"][token] = {"bids": [], "asks": []}

            if generate_json:
                all_api_events.extend(events)

            print(f"Page {page} processed: {len(events)} events evaluated.")
            current_cursor = data.get("next_cursor")
            
            # Break the loop if there are no more pages
            if not current_cursor or current_cursor == "LTE=":
                break

            page += 1

        except requests.exceptions.RequestException as e:
            print(f"\n[X] Critical HTTP request error: {e}")
            break

    # PHASE 2: Fetch Order Books concurrently to bypass the sequential bottleneck
    print(f"\n[*] Fetching {len(tokens_to_fetch)} order books concurrently for '{tag_slug}'...")
    fetched_order_books = {}
    
    # 50 workers provide a significant speedup while safely respecting the 150 req/sec API limit
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(get_order_book, session, token): token for token in tokens_to_fetch}
        for future in as_completed(futures):
            token, ob_data = future.result()
            fetched_order_books[token] = ob_data

    # PHASE 3: Re-inject the successfully fetched order books back into their parent market JSON objects
    if generate_json:
        for event in all_api_events:
            for market in event.get("markets", []):
                if not market.get("closed", False):
                    for token in market.get("clobTokenIds", []):
                        if token in fetched_order_books:
                            market["order_books"][token] = fetched_order_books[token]

    print(f"[✓] Extraction complete for '{tag_slug}'. Total markets mapped: {len(market_tokens_mapping)}\n")

    if generate_json:
        return market_tokens_mapping, all_api_events
    return market_tokens_mapping

def create_tag_file(session, category_tags):
    """
    Obtains categories tag IDs and stores them in a file, to save API calls.
    """
    print(f"--- Create cache file: {TAGS_FILE} ---")

    # Use the previous function to get the mapping of category slugs to their numeric IDs
    tags_map = get_tag_ids_by_slug_list(session, category_tags)
    
    data_to_save = tags_map
    
    
    with open(TAGS_FILE, "w", encoding="utf-8") as f:
        json.dump(data_to_save, f, indent=4, ensure_ascii=False)
    print(f"[✓] Tags file created successfully.\n")


def read_tag_file():
    """
    Reads tags from the local file.
    """
    if not os.path.exists(TAGS_FILE):
        return None
    
    with open(TAGS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
        # Return the categories mapping, or an empty dict if the file structure is unexpected
        return data

def create_json_output(file_name, data):
    """
    From the data obtained from the API, create a JSON file with a timestamp
    """
    now = datetime.now(timezone.utc)
    
    timestamp_str = now.strftime("%Y-%m-%d_%H-%M-%S")

    output_filename =  timestamp_str + file_name + ".json"

    print(f"\n--- Saving all data to {output_filename} ---")
    
    final_output = {
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "events": data
    }

    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=4, ensure_ascii=False)

def obtain_event_data(http_session, category_tag):
    """
    From an existing session, check if the tag file exists and obtain information about the events and markets for a category.
    """
    if not os.path.exists(TAGS_FILE):
        create_tag_file(http_session, category_tag)

    categories = read_tag_file()

    ids_categories_exclude = []
    all_collected_events = []
    total_market_mapping = {}

    for category in category_tag:
        mapping, events_json = get_markets_info(
            session=http_session,
            tag_slug=category, 
            ids_categories_exclude=ids_categories_exclude, 
            generate_json=True
        )
        
        total_market_mapping.update(mapping)
        all_collected_events.extend(events_json)

        # Append the current category ID to the exclusion list to prevent data duplication in subsequent iterations
        if categories[category]:
            ids_categories_exclude.append(categories[category])
    
    return total_market_mapping, all_collected_events


# ==========================================
# MAIN EXECUTION BLOCK
# ==========================================
if __name__ == "__main__":
    CATEGORIES_TAG = ["politics", "geopolitics", "tech", "finance", "economy"]
    
    # Configure the session with connection pooling to handle high-throughput multithreading
    http_session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    http_session.mount('https://', adapter)
    http_session.mount('http://', adapter)
    
    if not os.path.exists(TAGS_FILE):
        create_tag_file(http_session, CATEGORIES_TAG)

    categories = read_tag_file()

    ids_categories_exclude = []
    all_collected_events = []
    total_market_mapping = {}

    for category in CATEGORIES_TAG:
        mapping, events_json = get_markets_info(
            session=http_session,
            tag_slug=category, 
            ids_categories_exclude=ids_categories_exclude, 
            generate_json=True
        )
        
        total_market_mapping.update(mapping)
        all_collected_events.extend(events_json)

        # Append the current category ID to the exclusion list to prevent data duplication in subsequent iterations
        if categories[category]:
            ids_categories_exclude.append(categories[category])
            
    http_session.close()
    
    now = datetime.now(timezone.utc)
    
    timestamp_str = now.strftime("%Y-%m-%d_%H-%M-%S")

    output_filename = "events" + timestamp_str + ".json"

    print(f"\n--- Saving all data to {output_filename} ---")
    
    final_output = {
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "events": all_collected_events
    }

    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=4, ensure_ascii=False)

    print("\n[✓] Process successfully completed.")
    print(f"    - Total markets mapped: {len(total_market_mapping)}")
    print(f"    - Total events saved: {len(all_collected_events)}")