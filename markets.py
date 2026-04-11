import requests

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


def get_active_markets_by_slug(tag_slug):
    """
    Retrieves all active and open markets for a given category slug.
    It internally resolves the slug to a numeric ID before querying the markets endpoint.
    
    Args:
        tag_slug (str): The category identifier (e.g., 'tech').
        
    Returns:
        list: A list of strings containing the unique questions of the active markets.
    """
    # 1. Internal call to resolve the numeric ID
    tag_id = get_id_by_slug(tag_slug)
    
    if not tag_id:
        print(f"[X] Aborting: No valid ID obtained for '{tag_slug}'.")
        return []

    # 2. Fetch markets using the obtained ID
    url_markets = "https://gamma-api.polymarket.com/markets/keyset"
    
    current_cursor = None 
    total_markets = []
    page = 1
    
    print(f"Downloading ACTIVE markets for ID {tag_id}...\n")
    
    while True:
        print(f"Downloading page {page}...")
        
        # API query parameters
        params = {
            "tag_id": tag_id,
            "limit": 100,
            "active": "true",  # Filter 1: Only fetch active markets
            "closed": "false"  # Filter 2: Exclude resolved/closed markets
        }
        
        # Append pagination cursor if it exists from the previous iteration
        if current_cursor:
            params["after_cursor"] = current_cursor
            
        response = requests.get(url_markets, params=params)
        
        if response.status_code == 200:
            data = response.json()
            markets = data.get("markets", [])
            
            # Extract the question string from each market object
            for market in markets:
                question = market.get('question')
                # Safety check: ensure the question field exists and is not null
                if question: 
                    total_markets.append(question)
            
            # Check for the next pagination cursor
            current_cursor = data.get("next_cursor")
            
            # 'LTE=' or None signifies the end of the API dataset
            if not current_cursor or current_cursor == "LTE=":
                print("\n[✓] Download complete.")
                break
                
            page += 1
            
        else:
            print(f"\n[X] Request error: {response.status_code} - {response.text}")
            break
            
    # Remove any potential duplicates by converting to a set, then back to a list
    return list(set(total_markets))


def filter_insider_markets(market_list, keywords):
    """
    Filters a list of markets based on specific keywords that indicate
    sudden, confidential, or asymmetric events (e.g., M&A, bankruptcies).
    
    Args:
        market_list (list): List of market question strings.
        keywords (list): List of target keywords (case-insensitive).
        
    Returns:
        list: A filtered list of high-value market questions.
    """
    # Normalize all keywords to lowercase once to optimize performance
    normalized_keywords = [k.lower() for k in keywords]
    
    filtered_markets = []
    
    for market in market_list:
        # Convert the market question to lowercase for matching
        market_lower = market.lower()
        
        # Check against the pre-normalized keywords
        if any(keyword in market_lower for keyword in normalized_keywords):
            filtered_markets.append(market)
            
    return filtered_markets


def get_interesting_markets(category_tag, keywords):
    """
    Orchestrator function: Fetches all active markets for a given category 
    and filters them to find prime targets for anomaly detection.
    
    Args:
        category_tag (str): The Polymarket category (e.g., 'tech').
        keywords (list): Keywords associated with asymmetric information events.
        
    Returns:
        list: The final list of interesting markets ready for price/volume tracking.
    """
    # 1. Fetch all active markets for the selected category
    total_markets = get_active_markets_by_slug(category_tag)
    
    # 2. Apply the information asymmetry filter
    interesting_markets = filter_insider_markets(total_markets, keywords)

    return interesting_markets


# ==========================================
# MAIN EXECUTION BLOCK
# ==========================================
if __name__ == "__main__":
    
    # Configuration constants (easily swappable for other categories)
    CATEGORY_TAG = "tech"
    
    # Keywords indicating potential insider trading or sudden, closed-door events.
    KEYWORDS = [
        "bankruptcy", "acquire", "merger", "arrested", "jail", "charged",
        "out as", "next ceo", "incident", "disrupted", "outage", 
        "take a stake", "lawsuit", "sues", "banned"
    ]

    # Execute main pipeline
    interesting_markets = get_interesting_markets(CATEGORY_TAG, KEYWORDS)
    
    # Output results
    print(f"\nTOTAL INTERESTING MARKETS FOUND: {len(interesting_markets)}")
    print("-" * 50)
    
    for idx, market in enumerate(interesting_markets, 1):
        print(f"{idx}. {market}")