from __future__ import annotations

import csv
import re
from datetime import datetime, timezone
from pathlib import Path

import scrapy
from dateutil import parser as dateparser
from jpt_scraper.items import JptScraperItem

def clean_text(text: str | None) -> str:
    if not text:
        return ""
    return " ".join(text.split()).strip()

def read_last_date_from_csv(csv_path: str | None) -> str | None:
    """Reads max(published_date) from existing master CSV."""
    if not csv_path:
        return None
    path = Path(csv_path)
    if not path.exists():
        return None
    try:
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            dates = [r.get("published_date") for r in reader if r.get("published_date")]
        return max(dates) if dates else None
    except Exception:
        return None

class OilPriceLatestSpider(scrapy.Spider):
    name = "oilprice_latest"
    allowed_domains = ["oilprice.com"]
    start_urls = ["https://oilprice.com/Company-News/"]

    custom_settings = {
        "CONCURRENT_REQUESTS": 8,
        "AUTOTHROTTLE_ENABLED": True,
        "DOWNLOAD_DELAY": 0.5,
        "FEED_EXPORT_ENCODING": "utf-8",
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            r'C:\dev\pyhton_workspace\jpt_news\jpt_scraper\jpt_scraper\data\oilprice_master.csv': {
                'format': 'csv',
                'encoding': 'utf8',
                'store_empty': False,
            },
        },
    }

    def __init__(
        self,
        max_pages: int = 0,
        stop_at_last_date: int = 1, # Default to 1 to prevent duplicates
        csv_path: str = r'C:\dev\pyhton_workspace\jpt_news\jpt_scraper\jpt_scraper\data\oilprice_master.csv',
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.max_pages = int(max_pages)
        self.pages_seen = 0
        self.stop_at_last_date = int(stop_at_last_date)
        self.last_date = read_last_date_from_csv(csv_path) if self.stop_at_last_date else None

        if self.last_date:
            self.logger.info(f"Hard stop enabled. Last date in CSV: {self.last_date}")

    def parse(self, response: scrapy.http.Response):
        self.pages_seen += 1
        
        # Select all article containers
        articles = response.css("div.categoryArticle")
        
        for article in articles:
            # 1. Extract Date from meta string (e.g., "Feb 27, 2026 at 01:13...")
            meta_text = article.css("p.categoryArticle__meta::text").get()
            if not meta_text:
                continue
                
            try:
                # Split at ' at ' to get the date part
                date_part = meta_text.split(" at ")[0].strip()
                dt = dateparser.parse(date_part)
                published_date = dt.date().isoformat()
            except Exception:
                continue

            # HARD STOP LOGIC
            if self.last_date and published_date <= self.last_date:
                self.logger.info(f"Reached existing date {published_date}. Stopping crawl.")
                return

            # 2. Extract Title and URL
            title_node = article.css("h2.categoryArticle__title")
            title = clean_text(title_node.xpath("string()").get())
            
            relative_url = article.css("a.categoryArticle__imageHolder::attr(href)").get()
            url = response.urljoin(relative_url)

            # 3. Extract Company and Excerpt
            company = clean_text(article.css("p.categoryArticle__companyName::text").get())
            excerpt = clean_text(article.css("p.categoryArticle__excerpt::text").get())

            yield JptScraperItem(
                url=url,
                title=title,
                excerpt=excerpt,
                published_date=published_date,
                topics=["Company News"],
                tags=["OilPrice", company if company else "General"],
                scraped_at=datetime.now(timezone.utc).isoformat()
            )

        # Pagination Logic
        if self.max_pages == 0 or self.pages_seen < self.max_pages:
            next_page = response.css("a.next::attr(href)").get()
            if next_page:
                yield response.follow(next_page, callback=self.parse)
