from __future__ import annotations

from datetime import datetime, timezone

import scrapy
from dateutil import parser as dateparser

from jpt_scraper.items import JptScraperItem


API_URL = "https://oilprice.com/api/posts/category/company-news?page={page}"


class OilPriceLatestSpider(scrapy.Spider):
    name = "oilprice_latest"
    allowed_domains = ["oilprice.com"]

    custom_settings = {
        "CONCURRENT_REQUESTS": 4,
        "DOWNLOAD_DELAY": 0.4,
        "FEED_EXPORT_ENCODING": "utf-8",
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            r"C:\dev\pyhton_workspace\jpt_news\jpt_scraper\jpt_scraper\data\oilprice_master.csv": {
                "format": "csv",
                "encoding": "utf-8",
                "overwrite": True,
            }
        },
    }

    def start_requests(self):
        yield scrapy.Request(API_URL.format(page=1), meta={"page": 1})

    def parse(self, response: scrapy.http.Response):
        data = response.json()
        posts = data.get("posts", [])

        if not posts:
            return  # no more pages

        for post in posts:
            try:
                published_date = dateparser.parse(post["published_at"]).date().isoformat()
            except Exception:
                continue

            url = "https://oilprice.com" + post["url"]

            yield JptScraperItem(
                source="oilprice",
                url=url,
                title=post.get("title", "").strip(),
                excerpt=(post.get("excerpt") or "").strip(),
                published_date=published_date,
                company_name=(post.get("company") or "").strip(),
                topics=["Company News"],
                tags=["OilPrice"],
                scraped_at=datetime.now(timezone.utc).isoformat(),
                refresh_existing=0,
            )

        # Next page
        page = response.meta["page"] + 1
        yield scrapy.Request(API_URL.format(page=page), meta={"page": page})
