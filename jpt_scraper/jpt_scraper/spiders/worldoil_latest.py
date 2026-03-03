from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Iterable

import scrapy
from dateutil import parser as dateparser

from jpt_scraper.items import JptScraperItem

BASE = "https://www.worldoil.com"

START_URLS = [
    f"{BASE}/news",
    f"{BASE}/company-news",
    f"{BASE}/topics/onshore",
    f"{BASE}/topics/digital-transformation",
    f"{BASE}/topics/offshore",
    f"{BASE}/topics/energy-transition",
    f"{BASE}/topics/industry-analysis",
]

MONTH_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b"
)

TOPIC_WHITELIST = {
    "Onshore",
    "Offshore",
    "Digital Transformation",
    "Energy Transition",
    "Industry & Analysis",
}

# Optional: add a "Company News" topic label for /company-news items
COMPANY_NEWS_LABEL = "Company News"


def parse_date(text: str) -> str | None:
    m = MONTH_DATE_RE.search(text or "")
    if not m:
        return None
    try:
        return dateparser.parse(m.group(0)).date().isoformat()
    except Exception:
        return None


def clean_list(xs) -> list[str]:
    out = []
    seen = set()
    for x in xs or []:
        x = " ".join(str(x).split()).strip()
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


def split_topics_tags(labels: list[str]) -> tuple[list[str], list[str]]:
    topics, tags = [], []
    for x in clean_list(labels):
        if x in TOPIC_WHITELIST:
            topics.append(x)
        else:
            tags.append(x)
    return clean_list(topics), clean_list(tags)


def add_topic(topics: list[str], t: str) -> list[str]:
    topics = clean_list(topics)
    if t and t not in topics:
        topics.append(t)
    return topics


class WorldOilNewsSpider(scrapy.Spider):
    name = "worldoil_latest"
    allowed_domains = ["www.worldoil.com", "worldoil.com"]
    start_urls = START_URLS

    custom_settings = {
        "CONCURRENT_REQUESTS": 12,
        "AUTOTHROTTLE_ENABLED": True,
        "DOWNLOAD_DELAY": 0.3,
        "ROBOTSTXT_OBEY": True,
        "FEED_EXPORT_ENCODING": "utf-8",
        "LOG_LEVEL": "INFO",
    }

    def __init__(self, max_pages: int = 0, stop_at_last_date: int = 0, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_pages = int(max_pages)
        self.pages_seen = 0
        self.stop_at_last_date = int(stop_at_last_date)

        # Prevent duplicates across multiple start sections & pagination
        self.seen_urls: set[str] = set()

    def parse(self, response: scrapy.http.Response):
        """
        Handles list pages for:
          /news
          /company-news
          /topics/*
        WorldOil uses the same "news-row" pattern across these sections.
        """
        self.pages_seen += 1

        section = response.url
        is_company_news_section = "/company-news" in section

        for row in response.css("div.news-row"):
            href = row.css("div.news-title a::attr(href)").get()
            if not href:
                continue
            url = response.urljoin(href)

            # ---- de-dupe at crawl time (don’t even schedule the request)
            if url in self.seen_urls:
                continue
            self.seen_urls.add(url)

            title = " ".join(row.css("div.news-title h2::text").getall()).strip()
            date_text = " ".join(row.css("div.news-date::text").getall()).strip()
            published_date = parse_date(date_text)
            if not published_date:
                continue

            labels = row.css("div.content-topics a::text").getall()
            topics, tags = split_topics_tags(labels)

            # If it came from /company-news, optionally tag it as a topic
            if is_company_news_section:
                topics = add_topic(topics, COMPANY_NEWS_LABEL)

            # (Hard stop behavior is handled in your script version; leaving off here for now)

            yield response.follow(
                url,
                callback=self.parse_article,
                meta={
                    "url": url,
                    "title": title,
                    "published_date": published_date,
                    "topics": topics,
                    "tags": tags,
                },
            )

        # Pagination:
        # Your current selector works for /news; this fallback makes it work more broadly.
        if self.max_pages == 0 or self.pages_seen < self.max_pages:
            next_href = (
                response.css("ul.pagination li.page-item.active + li.page-item a.page-link::attr(href)").get()
                or response.css("ul.pagination a.page-link i.bi-chevron-right").xpath("../@href").get()
                or response.css('a[rel="next"]::attr(href)').get()
            )

            if next_href:
                yield response.follow(next_href, callback=self.parse)

    def parse_article(self, response: scrapy.http.Response):
        url = response.meta["url"]
        title = response.meta.get("title") or ""
        published_date = response.meta["published_date"]
        topics = response.meta.get("topics") or []
        tags = response.meta.get("tags") or []

        excerpt = response.css('meta[name="description"]::attr(content)').get() or ""
        excerpt = " ".join(excerpt.split()).strip()

        if not excerpt:
            p = response.css("article p::text, .article p::text, .content p::text").get()
            excerpt = " ".join((p or "").split()).strip()

        yield JptScraperItem(
            source="worldoil",
            url=url,
            title=title,
            excerpt=excerpt,
            published_date=published_date,
            topics=clean_list(topics),
            tags=clean_list(tags),
            scraped_at=datetime.now(timezone.utc).isoformat(),
            refresh_existing=0,
        )
