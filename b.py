import os
import hashlib
import time
import json
import redis
import duckdb
import requests
import concurrent.futures
from datetime import datetime
from bs4 import BeautifulSoup
import random

# --- Redis Configuration ---
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_QUEUE = "newegg_queue"

# --- DuckDB Configuration ---
DUCKDB_FILENAME = "products.duckdb"

# --- Bright Data Configuration ---
BRIGHTDATA_USER = "brd-customer-hl_b56f4b70-zone-web_unlocker1"
BRIGHTDATA_PASS = "vo1zdu0gglm3"
BRIGHTDATA_HOST = "brd.superproxy.io"
BRIGHTDATA_PORT = "33335"

# --- Cache Utility Functions ---
def get_cache_filename(url):
    """Generate a cache filename based on the URL's MD5 hash."""
    h = hashlib.md5(url.encode("utf-8")).hexdigest()
    return os.path.join("cache", f"{h}.html")

def get_cached_html(url):
    """Return cached HTML content if it exists, otherwise None."""
    filename = get_cache_filename(url)
    if os.path.exists(filename):
        print(f"Using cached HTML from: {filename}")
        with open(filename, "r", encoding="utf-8") as f:
            return f.read()
    return None

def save_html_cache(url, html):
    """Save HTML content to cache."""
    os.makedirs("cache", exist_ok=True)
    filename = get_cache_filename(url)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)

# -------------------------------------------------------------------
#                     JSON-LD PARSING HELPERS
# -------------------------------------------------------------------

def extract_from_ld_item(item):
    """
    Extract product details from a JSON-LD item for a Product.
    Returns a dict with keys: name, description, brand, reviewCount, price.
    """
    result = {}
    if item.get("@type") == "Product":
        result["name"] = item.get("name")
        result["description"] = item.get("description")
        # Brand can be a dict or a string.
        brand = item.get("brand")
        if isinstance(brand, dict):
            result["brand"] = brand.get("name")
        else:
            result["brand"] = brand
        # Aggregate rating (if present)
        aggregateRating = item.get("aggregateRating")
        if isinstance(aggregateRating, dict):
            result["reviewCount"] = aggregateRating.get("reviewCount")
        # Offers (if present)
        offers = item.get("offers")
        if isinstance(offers, dict):
            result["price"] = offers.get("price")
    return result

def parse_json_ld_data(soup):
    """
    Parse JSON-LD data for a product.
    Returns a dictionary with keys: name, description, brand, reviewCount, price.
    """
    ld_data = {}
    ld_scripts = soup.find_all("script", {"type": "application/ld+json"})
    for ld_script in ld_scripts:
        try:
            data = json.loads(ld_script.string or "")
            items = []
            if isinstance(data, dict):
                items = [data]
            elif isinstance(data, list):
                items = data
            for item in items:
                if item.get("@type") == "Product":
                    extracted = extract_from_ld_item(item)
                    # Merge fields if not already set
                    for key, value in extracted.items():
                        if value and key not in ld_data:
                            ld_data[key] = value
        except (json.JSONDecodeError, TypeError):
            continue
    return ld_data

# -------------------------------------------------------------------
#                     HTML PARSING HELPERS
# -------------------------------------------------------------------

def parse_specs_table(soup):
    """
    Parse the specs table <dl class="product-specs"> for key-value pairs.
    Returns a dict (e.g., {"Brand": "AMD", ...}).
    """
    specs = {}
    specs_table = soup.select_one("dl.product-specs")
    if not specs_table:
        return specs

    dt_elements = specs_table.find_all("dt")
    for dt in dt_elements:
        key = dt.get_text(strip=True)
        dd = dt.find_next_sibling("dd")
        if dd:
            value = dd.get_text(strip=True)
            specs[key] = value
    return specs

def parse_product_details(html, url):
    """
    Extract product details (title, brand, price, reviews count, description)
    from HTML using BeautifulSoup. JSON-LD data is preferred when available.
    """
    soup = BeautifulSoup(html, "html.parser")
    ld_data = parse_json_ld_data(soup)
    
    # Title: use JSON-LD "name" if available, else fallback to HTML.
    html_title_el = soup.find("h1", class_="product-title")
    html_title = html_title_el.get_text(strip=True) if html_title_el else None
    title = ld_data.get("name") or html_title

    # Brand: prefer JSON-LD, then specs table, then fallback selector.
    ld_brand = ld_data.get("brand")
    specs = parse_specs_table(soup)
    specs_brand = specs.get("Brand")
    brand_el = soup.select_one("div.product-brand a")
    fallback_brand = brand_el.get_text(strip=True) if brand_el else None
    brand = ld_brand or specs_brand or fallback_brand

    # Price: prefer JSON-LD offers price, else fallback to HTML.
    ld_price = ld_data.get("price")
    price_el = soup.select_one("li.price-current, div.price-current")
    html_price = price_el.get_text(strip=True) if price_el else None
    price = ld_price or html_price

    # Reviews Count: prefer JSON-LD, then try selectors.
    ld_review_count = ld_data.get("reviewCount")
    review_count = ld_review_count
    if not review_count:
        reviews_el = soup.select_one("span.rating-num, span[itemprop='reviewCount']")
        if reviews_el:
            review_count = reviews_el.get_text(strip=True)
    if not review_count:
        reviews_el = soup.select_one("div.product-rating > span.item-rating-num")
        if reviews_el:
            review_text = reviews_el.get_text(strip=True)
            review_count = review_text.strip("()")
    
    # Description: prefer JSON-LD description, else parse bullet points.
    ld_description = ld_data.get("description")
    desc_el = soup.select_one("div.product-bullets")
    if desc_el:
        bullets = [li.get_text(strip=True) for li in desc_el.select("ul li")]
        html_description = "\n".join(bullets) if bullets else desc_el.get_text("\n", strip=True)
    else:
        html_description = None
    description = ld_description or html_description

    return {
        "url": url,
        "title": title,
        "brand": brand,
        "price": price,
        "reviews_count": review_count,
        "description": description,
        "scraped_at": datetime.now().isoformat()
    }

# -------------------------------------------------------------------
#                    DUCKDB STORAGE FUNCTIONS
# -------------------------------------------------------------------

def init_duckdb():
    """
    Initialize a DuckDB connection and create the products table if it doesn't exist.
    """
    con = duckdb.connect(DUCKDB_FILENAME)
    con.execute("""
        CREATE TABLE IF NOT EXISTS products (
            url TEXT PRIMARY KEY,
            title TEXT,
            brand TEXT,
            price TEXT,
            reviews_count TEXT,
            description TEXT,
            scraped_at TIMESTAMP
        )
    """)
    return con

def store_product_details(con, details):
    """
    Store product details in the DuckDB table.
    """
    con.execute("""
        INSERT OR REPLACE INTO products (url, title, brand, price, reviews_count, description, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (details.get("url"), details.get("title"), details.get("brand"),
          details.get("price"), details.get("reviews_count"),
          details.get("description"), details.get("scraped_at")))
    con.commit()

# -------------------------------------------------------------------
#                    MAIN SCRAPING LOGIC
# -------------------------------------------------------------------

def scrape_product_info(url):
    """
    Main function that:
      1. Checks for cached HTML.
      2. If not cached, uses Bright Data proxy to fetch the page.
      3. Parses and returns a dict of product details.
    """
    cached_html = get_cached_html(url)
    if cached_html:
        print(f"Using cached HTML content for: {url}")
        return parse_product_details(cached_html, url)
    
    # Configure Bright Data proxy
    proxy_url = f"http://{BRIGHTDATA_USER}:{BRIGHTDATA_PASS}@{BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
    }

    try:
        print(f"Fetching URL via Bright Data: {url}")
        response = requests.get(
            url,
            headers=headers,
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=30,
            verify=False  # SSL verification disabled (use with caution)
        )

        if response.status_code == 200:
            print(f"Successfully fetched: {url}")
            html_content = response.text
            save_html_cache(url, html_content)
            return parse_product_details(html_content, url)
        else:
            error_msg = f"HTTP status {response.status_code}"
            print(f"Failed to fetch {url}: {error_msg}")
            return {"url": url, "error": error_msg}

    except Exception as e:
        error_msg = str(e)
        print(f"Exception occurred while fetching {url}: {error_msg}")
        return {"url": url, "error": error_msg}

# -------------------------------------------------------------------
#                MAIN RUNNER (Concurrent & Redis Queue)
# -------------------------------------------------------------------

def process_url(url, duck_con):
    info = scrape_product_info(url)
    if "error" in info:
        print(f"Error scraping {url}: {info['error']}")
    else:
        print(f"Scraped data for {url}:")
        print(f"  Title:         {info.get('title')}")
        print(f"  Brand:         {info.get('brand')}")
        print(f"  Price:         {info.get('price')}")
        print(f"  Reviews Count: {info.get('reviews_count')}")
        print(f"  Description:   {info.get('description')}")
        store_product_details(duck_con, info)
    print("-" * 50 + "\n")
    # Optional delay to throttle requests
    time.sleep(random.uniform(1, 3))

def main():
    # Connect to Redis and retrieve all URLs from the queue.
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    print("Connected to Redis. Popping URLs from queue:", REDIS_QUEUE)
    
    urls = []
    while True:
        url = r.lpop(REDIS_QUEUE)
        if url is None:
            break
        urls.append(url.decode("utf-8"))
    
    if not urls:
        print("No URLs found in Redis queue. Exiting.")
        return

    # Initialize DuckDB connection
    duck_con = init_duckdb()
    
    # Use ThreadPoolExecutor for concurrent scraping.
    max_workers = 3  # Adjust based on load and ethical scraping practices.
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_url, url, duck_con): url for url in urls}
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"URL {futures[future]} generated an exception: {exc}")
    
    duck_con.close()
    print("All tasks completed. DuckDB connection closed.")

if __name__ == "__main__":
    main()
