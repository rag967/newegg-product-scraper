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
import urllib3
from urllib.parse import quote

# Disable SSL warnings (use with caution)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_QUEUE = "newegg_queue"

DUCKDB_FILENAME = "products.duckdb"

# Bright Data proxy configuration
BRIGHTDATA_USER = "brd-customer-hl_b56f4b70-zone-web_unlocker1"
BRIGHTDATA_PASS = "vo1zdu0gglm3"
BRIGHTDATA_HOST = "brd.superproxy.io"
BRIGHTDATA_PORT = "33335"

# --- Cache Utility Functions ---
def get_cache_filename(url):
    h = hashlib.md5(url.encode("utf-8")).hexdigest()
    return os.path.join("cache", f"{h}.html")

def get_cached_html(url):
    filename = get_cache_filename(url)
    if os.path.exists(filename):
        print(f"Using cached HTML from: {filename}")
        with open(filename, "r", encoding="utf-8") as f:
            return f.read()
    return None

def save_html_cache(url, html):
    os.makedirs("cache", exist_ok=True)
    filename = get_cache_filename(url)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)

# --- JSON-LD Parsing Helpers ---
def extract_from_ld_item(item):
    result = {}
    if item.get("@type") == "Product":
        result["name"] = item.get("name")
        result["description"] = item.get("description")
        brand = item.get("brand")
        if isinstance(brand, dict):
            result["brand"] = brand.get("name")
        else:
            result["brand"] = brand
        aggregateRating = item.get("aggregateRating")
        if isinstance(aggregateRating, dict):
            result["reviewCount"] = aggregateRating.get("reviewCount")
            result["aggregateRating"] = aggregateRating.get("ratingValue")
        offers = item.get("offers")
        if isinstance(offers, dict):
            result["price"] = offers.get("price")
    return result

def parse_json_ld_data(soup):
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
                    for key, value in extracted.items():
                        if value and key not in ld_data:
                            ld_data[key] = value
        except (json.JSONDecodeError, TypeError):
            continue
    return ld_data

# --- HTML Parsing Helpers ---
def parse_specs_table(soup):
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

def get_newegg_item_number(url):
    # Extract the NeweggItemNumber from the URL (the part after "/p/")
    parts = url.split("/p/")
    if len(parts) > 1:
        return parts[1].split('?')[0].strip()
    return None

def parse_product_details(html, url):
    soup = BeautifulSoup(html, "html.parser")
    ld_data = parse_json_ld_data(soup)
    
    html_title_el = soup.find("h1", class_="product-title")
    html_title = html_title_el.get_text(strip=True) if html_title_el else None
    title = ld_data.get("name") or html_title

    ld_brand = ld_data.get("brand")
    specs = parse_specs_table(soup)
    specs_brand = specs.get("Brand")
    brand_el = soup.select_one("div.product-brand a")
    fallback_brand = brand_el.get_text(strip=True) if brand_el else None
    brand = ld_brand or specs_brand or fallback_brand

    ld_price = ld_data.get("price")
    price_el = soup.select_one("li.price-current, div.price-current")
    html_price = price_el.get_text(strip=True) if price_el else None
    price = ld_price or html_price

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
    
    ld_description = ld_data.get("description")
    desc_el = soup.select_one("div.product-bullets")
    if desc_el:
        bullets = [li.get_text(strip=True) for li in desc_el.select("ul li")]
        html_description = "\n".join(bullets) if bullets else desc_el.get_text("\n", strip=True)
    else:
        html_description = None
    description = ld_description or html_description

    # Try to get the item number from the specs table.
    item_number = specs.get("Item Number") or specs.get("Item#") or ""
    newegg_item_number = get_newegg_item_number(url)
    if not item_number and newegg_item_number and len(newegg_item_number) >= 8:
        candidate = newegg_item_number[7:]
        if len(candidate) == 8:
            formatted = candidate[:2] + "-" + candidate[2:5] + "-" + candidate[5:]
            item_number = formatted

    # Extract ItemGroupId and SubCategoryId
    item_group_meta = soup.find("meta", {"itemprop": "itemGroupId"})
    item_group_id = item_group_meta.get("content") if item_group_meta else None

    category_meta = soup.find("meta", {"itemprop": "categoryId"})
    sub_category_id = category_meta.get("content") if category_meta else None
    return {
        "url": url,
        "title": title,
        "brand": brand,
        "price": price,
        "reviews_count": review_count,
        "average_rating": ld_data.get("aggregateRating"),
        "description": description,
        "item_number": item_number,
        "newegg_item_number": newegg_item_number,
        "scraped_at": datetime.now().isoformat(),
        "item_group_id": item_group_id,
        "sub_category_id": sub_category_id,
    }

# --- Review API Parsing ---
def parse_review_api_response(response_json):
    """
    Parse the review API JSON response.
    Returns a tuple: (product_info, reviews)
    """
    product_info = {}
    reviews = []
    if not isinstance(response_json, dict):
        print("Review API response is not a dictionary:", response_json)
        return product_info, reviews

    search_result = response_json.get("SearchResult", {})
    if not isinstance(search_result, dict):
        search_result = {}
    
    customer_reviews = search_result.get("CustomerReviewList", [])
    if customer_reviews and isinstance(customer_reviews, list):
        first_review = customer_reviews[0]
        product_info["title"] = first_review.get("ItemDescription", "").strip()
        product_info["brand"] = first_review.get("BrandDescription", "").strip()
        product_info["reviews_count"] = first_review.get("TotalReviews", None)
        product_info["description"] = first_review.get("ItemDescription", "").strip()
    review_filter = search_result.get("ReivewFilter", {})
    if not isinstance(review_filter, dict):
        review_filter = {}
    product_info["average_rating"] = review_filter.get("AverageRatingFloat", None)

    for review in customer_reviews:
        if not isinstance(review, dict):
            continue
        review_data = {
            "reviewer_name": review.get("DisplayName", "").strip(),
            "rating": review.get("Rating", None),
            "review_title": review.get("Title", "").strip(),
            "review_body": review.get("Comments", "").strip(),
            "review_date": review.get("InDate", ""),
            "verified_buyer": review.get("HasPurchased", False),
            "pros": review.get("Pros", "").strip(),
            "cons": review.get("Cons", "").strip(),
            "total_voting": review.get("TotalVoting", 0),
            "total_consented": review.get("TotalConsented", 0),
        }
        reviews.append(review_data)

    return product_info, reviews

def scrape_product_reviews(product_details):
    """
    Build the review API request using product details and fetch the review JSON.
    """
    item_number = product_details.get("item_number", "")
    newegg_item_number = product_details.get("newegg_item_number", "")
    review_request = {
        "IsGetSummary": True,
        "IsGetTopReview": False,
        "IsGetItemProperty": False,
        "IsGetAllReviewCategory": False,
        "IsSearchWithoutStatistics": False,
        "IsGetFilterCount": False,
        "IsGetFeatures": True,
        "SearchProperty": {
            "CombineGroup": 2,
            "FilterDate": 0,
            "IsB2BExclusiveReviews": False,
            "IsBestCritialReview": False,
            "IsBestFavorableReview": False,
            "IsItemMarkOnly": True,
            "IsProductReviewSearch": True,
            "IsPurchaserReviewOnly": False,
            "IsResponsiveSite": False,
            "IsSmartPhone": False,
            "IsVendorResponse": False,
            "IsVideoReviewOnly": False,
            "ItemGroupId": product_details.get("item_group_id", 200826006),
            "ItemNumber": item_number,
            "NeweggItemNumber": newegg_item_number,
            "PageIndex": 1,
            "PerPageItemCount": 100,
            "RatingReviewDisplayType": 0,
            "ReviewTimeFilterType": 0,
            "RatingType": -1,
            "ReviewType": 3,
            "SearchKeywords": "",
            "SearchLanguage": "",
            "SellerId": "",
            "SortOrderType": 1,
            "SubCategoryId": "343",
            "TransNumber": 0,
            "WithImage": False,
            "HotKeyword": ""
        }
    }
    review_request_str = quote(json.dumps(review_request))
    review_api_url = f"https://www.newegg.com/product/api/ProductReview?reviewRequestStr={review_request_str}"
    
    proxy_url = f"http://{BRIGHTDATA_USER}:{BRIGHTDATA_PASS}@{BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9"
    }
    
    try:
        print(f"Fetching reviews via Bright Data: {review_api_url}")
        response = requests.get(
            review_api_url,
            headers=headers,
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=30,
            verify=False
        )
        if response.status_code == 200:
            review_json = response.json()
            if not isinstance(review_json, dict):
                print("Review API returned a non-dict response:", review_json)
                return {}, []
            prod_info, reviews = parse_review_api_response(review_json)
            return prod_info, reviews
        else:
            print(f"Failed to fetch reviews: HTTP {response.status_code}")
            return {}, []
    except Exception as e:
        print(f"Exception occurred while fetching reviews: {str(e)}")
        return {}, []

# --- DuckDB Storage Functions ---
def init_duckdb():
    con = duckdb.connect(DUCKDB_FILENAME)
    # Drop dependent tables first.
    try:
        con.execute("DROP TABLE IF EXISTS reviews")
    except Exception as e:
        print("Error dropping table 'reviews':", e)
    con.execute("DROP TABLE IF EXISTS product_reviews")
    con.execute("DROP TABLE IF EXISTS products")
    con.execute("""
        CREATE TABLE products (
            url TEXT PRIMARY KEY,
            title TEXT,
            brand TEXT,
            price TEXT,
            reviews_count TEXT,
            average_rating TEXT,
            description TEXT,
            item_number TEXT,
            newegg_item_number TEXT,
            scraped_at TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE product_reviews (
            product_url TEXT,
            reviewer_name TEXT,
            rating INTEGER,
            review_title TEXT,
            review_body TEXT,
            review_date TEXT,
            verified_buyer BOOLEAN,
            pros TEXT,
            cons TEXT,
            total_voting INTEGER,
            total_consented INTEGER,
            scraped_at TIMESTAMP,
            PRIMARY KEY (product_url, reviewer_name, review_date)
        )
    """)
    return con

def store_product_details(con, details):
    con.execute("""
        INSERT OR REPLACE INTO products (url, title, brand, price, reviews_count, average_rating, description, item_number, newegg_item_number, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (details.get("url"), details.get("title"), details.get("brand"),
          details.get("price"), details.get("reviews_count"), details.get("average_rating"),
          details.get("description"), details.get("item_number"), details.get("newegg_item_number"),
          details.get("scraped_at")))
    con.commit()

def store_review_details(con, product_url, review):
    con.execute("""
        INSERT OR REPLACE INTO product_reviews (product_url, reviewer_name, rating, review_title, review_body, review_date, verified_buyer, pros, cons, total_voting, total_consented, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (product_url, review.get("reviewer_name"), review.get("rating"), review.get("review_title"),
          review.get("review_body"), review.get("review_date"), review.get("verified_buyer"),
          review.get("pros"), review.get("cons"), review.get("total_voting"), review.get("total_consented"),
          datetime.now().isoformat()))
    con.commit()

# --- Main Scraping Logic ---
def scrape_product_info(url):
    cached_html = get_cached_html(url)
    if cached_html:
        print(f"Using cached HTML content for: {url}")
        return parse_product_details(cached_html, url)
    
    proxy_url = f"http://{BRIGHTDATA_USER}:{BRIGHTDATA_PASS}@{BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
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
            verify=False
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
        print(f"  Average Rating:{info.get('average_rating')}")
        print(f"  Description:   {info.get('description')}")
        store_product_details(duck_con, info)
        
        prod_info_reviews, reviews = scrape_product_reviews(info)
        if reviews:
            print(f"Scraped {len(reviews)} reviews for {url}")
            # Print each review's details
            for idx, review in enumerate(reviews, start=1):
                print(f"\nReview {idx}:")
                print(f"  Reviewer Name:  {review.get('reviewer_name')}")
                print(f"  Rating:         {review.get('rating')}")
                print(f"  Review Title:   {review.get('review_title')}")
                print(f"  Review Body:    {review.get('review_body')}")
                print(f"  Review Date:    {review.get('review_date')}")
                print(f"  Verified Buyer: {review.get('verified_buyer')}")
                print(f"  Additional Details: Pros: {review.get('pros')}, Cons: {review.get('cons')}, Voting: {review.get('total_voting')}, Consented: {review.get('total_consented')}")
                print("-" * 50)
            # Optionally update product info from review summary
            if prod_info_reviews and isinstance(prod_info_reviews, dict) and prod_info_reviews.get("average_rating"):
                info["average_rating"] = prod_info_reviews.get("average_rating")
                info["reviews_count"] = prod_info_reviews.get("reviews_count", info.get("reviews_count"))
                store_product_details(duck_con, info)
            # Store reviews into DuckDB
            for review in reviews:
                store_review_details(duck_con, url, review)
        else:
            print("No reviews scraped.")
    print("-" * 50 + "\n")
    time.sleep(random.uniform(1, 3))

def main():
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
    duck_con = init_duckdb()
    max_workers = 3
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

