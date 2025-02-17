# README.md
# Newegg Product Scraper

A robust web scraper for Newegg product pages with the following features:

- Advanced scraping techniques (JSON-LD parsing, HTML parsing, caching)
- Concurrent processing using Redis queue and ThreadPoolExecutor
- DuckDB storage for scraped data
- Captcha handling capability
- Rate limiting and ethical scraping practices

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
playwright install chromium
