# push_urls.py
import redis

def push_urls_to_redis(urls, queue_name="newegg_queue"):
    """
    Push each URL in `urls` to the specified Redis list (queue).
    """
    r = redis.Redis(host="localhost", port=6379, db=0)
    for url in urls:
        r.rpush(queue_name, url)
    print(f"Pushed {len(urls)} URLs to Redis list '{queue_name}'.")

if __name__ == "__main__":
    # Replace with your list of URLs
    urls_to_push = [
        "https://www.newegg.com/amd-ryzen-9-9900x-ryzen-9-9000-series-granite-ridge-socket-am5-processor/p/N82E16819113842",
        "https://www.newegg.com/intel-core-i7-14700k-core-i7-14th-gen-raptor-lake-lga-1700-desktop-processor/p/N82E16819118466",
        #"https://www.newegg.com/intel-core-i9-14900ks-core-i9-14th-gen-raptor-lake-lga-1700-desktop-processor/p/N82E16819118496",
        #"https://www.newegg.com/amd-ryzen-9-5950x-ryzen-9-5000-series-vermeer-socket-am4-desktop-processor/p/N82E16819113663",
        #"https://www.newegg.com/intel-core-i5-14400-core-i5-14th-gen-raptor-lake-lga-1700-processor/p/N82E16819118480",
        #"https://www.newegg.com/intel-core-i7-14700k-core-i7-14th-gen-raptor-lake-lga-1700-desktop-processor/p/N82E16819118466",
        #"https://www.newegg.com/amd-ryzen-5-5500-ryzen-5-5000-series/p/N82E16819113737",
    ]
    push_urls_to_redis(urls_to_push)

