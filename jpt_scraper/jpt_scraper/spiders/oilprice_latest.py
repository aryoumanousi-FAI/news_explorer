from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path

import scrapy
from scrapy.crawler import CrawlerProcess
from dateutil import parser as dateparser

# -------------------
# PATHS & CONFIG
# -------------------
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent
DATA_DIR = REPO_ROOT / "jpt_scraper" / "data"
DAILY_CSV = DATA_DIR / "oilprice_daily.csv"

MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))

# -------------------
# HELPERS
# -------------------
# Matches dates like "Feb 26, 2026"
DATE_RE = re.compile(r"([A-Z][a-z]{2}\s\d{1,2},\s\d{4})")

def parse_date(text: str) -> str | None:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    try:
        return dateparser.parse(m.group(1)).date().isoformat()
    except Exception:
        return None

# -------------------
# SPIDER
# -------------------
class OilPriceSpider(scrapy.Spider):
    name = "oilprice_company_news"
    allowed_domains = ["oilprice.com"]
    start_urls = ["https://oilprice.com/Company-News/"]

    def __init__(self, max_pages: int = 10, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_pages = int(max_pages)
        self.pages_seen = 0

    def parse(self, response: scrapy.http.Response):
        self.pages_seen += 1

        for article in response.css("div.categoryArticle"):
            company_name = article.css("p.categoryArticle__companyName::text").get()
            title = article.css("h2.categoryArticle__title::text").get()
            link = article.css("a::attr(href)").get()
            excerpt = article.css("p.categoryArticle__excerpt::text").get()
            meta_info = article.css("p.categoryArticle__meta::text").get()

            if company_name:
                # Yielding a standard dictionary instead of JptScraperItem to easily 
                # accommodate the new `company_name` field without editing items.py
                yield {
                    "source": "OilPrice",
                    "company_name": company_name.strip(),
                    "title": title.strip() if title else "",
                    "url": response.urljoin(link) if link else "",
                    "excerpt": " ".join((excerpt or "").split()).strip(),
                    "published_date": parse_date(meta_info),
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                }

        # Follow pagination
        if self.max_pages == 0 or self.pages_seen < self.max_pages:
            next_page = response.css("div.pagination a.next::attr(href)").get()
            if next_page:
                yield response.follow(next_page, callback=self.parse)

# -------------------
# RUNNER
# -------------------
def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    if DAILY_CSV.exists():
        DAILY_CSV.unlink()

    print("--- Scrape step (daily only) ---")
    print(f"Spider:      OilPriceSpider")
    print(f"MAX_PAGES:   {MAX_PAGES}")
    print(f"Output:      {DAILY_CSV}")

    # Set up Scrapy to run directly from this script
    process = CrawlerProcess(settings={
        "CONCURRENT_REQUESTS": 12,
        "AUTOTHROTTLE_ENABLED": True,
        "DOWNLOAD_DELAY": 0.5,
        "ROBOTSTXT_OBEY": True,
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            str(DAILY_CSV): {
                "format": "csv",
                "encoding": "utf8",
                "overwrite": True,
            }
        },
    })

    # Pass the MAX_PAGES variable directly into the spider
    process.crawl(OilPriceSpider, max_pages=MAX_PAGES)
    process.start()


if __name__ == "__main__":
    main()
