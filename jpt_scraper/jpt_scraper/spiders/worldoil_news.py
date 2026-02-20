from __future__ import annotations

import csv
import re
from datetime import datetime, timezone
from pathlib import Path

import scrapy
from dateutil import parser as dateparser

from jpt_scraper.items import JptScraperItem

BASE = "https://www.worldoil.com"
START_URL = f"{BASE}/news"

MONTH_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b"
)

# Your explicit topic taxonomy (expand anytime)
TOPIC_WHITELIST = {
    "onshore",
    "offshore",
    "digital transformation",
    "energy transition",
    "industry & analysis",
}

def parse_date_from_text(text: str) -> str | None:
    m = MONTH_DATE_RE.search(text or "")
    if not m:
        return None
    try:
        dt = dateparser.parse(m.group(0))
        return dt.date().isoformat()
    except Exception:
        return None

def clean_list(xs) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for x in xs or []:
        x = " ".join(str(x).split()).strip()
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out

def read_last_date_from_csv(csv_path: str | None, source: str) -> str | None:
    if not csv_path:
        return None
    path = Path(csv_path)
    if not path.exists():
        return None

    try:
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            dates = []
            for r in reader:
                if r.get("source") != source:
                    continue
                d = r.get("published_date")
                if d:
                    dates.append(d)
        return max(dates) if dates else None
    except Exception:
        return None

def split_topics_tags(labels: list[str]) -> tuple[list[str], list[str]]:
    topics: list[str] = []
    tags: list[str] = []
    for raw in clean_list(labels):
        key = " ".join(raw.split()).strip().lower()
        if key in TOPIC_WHITELIST:
            topics.append(raw)   # keep original display text
        else:
            tags.append(raw)
    return clean_list(topics), clean_list(tags)

class WorldOilNewsSpider(scrapy.Spider):
    name = "worldoil_news"
    allowed_domains = ["www.worldoil.com", "worldoil.com"]
    start_urls = [START_URL]

    custom_settings = {
        "CONCURRENT_REQUESTS": 12,
        "AUTOTHROTTLE_ENABLED": True,
        "DOWNLOAD_DELAY": 0.3,
        "ROBOTSTXT_OBEY": True,
        "FEED_EXPORT_ENCODING": "utf-8",
        "LOG_LEVEL": "INFO",
    }

    def __init__(
        self,
        max_pages: int = 0,
        stop_at_last_date: int = 0,
        csv_path: str | None = None,
        refresh_existing: int = 0,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.max_pages = int(max_pages)
        self.pages_seen = 0
        self.refresh_existing = int(refresh_existing)

        self.stop_at_last_date = int(stop_at_last_date)
        self.last_date = read_last_date_from_csv(csv_path, source="worldoil") if self.stop_at_last_date else None
        if self.last_date:
            self.logger.info(f"Hard stop enabled. Last date in CSV for worldoil: {self.last_date}")

    def parse(self, response: scrapy.http.Response):
        self.pages_seen += 1

        for row in response.css("div.news-row"):
            href = row.css("div.news-title a::attr(href)").get()
            if not href:
                continue
            url = response.urljoin(href)

            title = " ".join(row.css("div.news-title h2::text").getall()).strip()

            date_text = " ".join(row.css("div.news-date::text").getall()).strip()
            published_date = parse_date_from_text(date_text)
            if not published_date:
                continue

            # Grab *all* labels from the list row (topics/countries/tags)
            labels = row.css("div.content-topics a::text").getall()
            topics, tags = split_topics_tags(labels)

            # HARD STOP for paging
            if self.last_date and published_date <= self.last_date:
                self.logger.info(f"Reached existing date {published_date} <= {self.last_date}. Stopping.")
                return

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

        # Pagination: /news?page=2
        if self.max_pages == 0 or self.pages_seen < self.max_pages:
            next_href = response.css(
                "ul.pagination li.page-item.active + li.page-item a.page-link::attr(href)"
            ).get()
            if not next_href:
                # fallback: icon-based next
                next_href = response.css("ul.pagination a.page-link i.bi-chevron-right").xpath("../@href").get()

            if next_href:
                yield response.follow(next_href, callback=self.parse)

    def parse_article(self, response: scrapy.http.Response):
        url = response.meta["url"]
        title = response.meta.get("title") or ""
        published_date = response.meta["published_date"]

        topics = list(response.meta.get("topics") or [])
        tags = list(response.meta.get("tags") or [])

        # Try to capture any additional labels from the article page too (if present)
        article_labels = response.css("a[href^='/topics/']::text").getall()
        t2, g2 = split_topics_tags(article_labels)
        topics = clean_list(topics + t2)
        tags = clean_list(tags + g2)

        # Excerpt from meta description first
        excerpt = response.css('meta[name="description"]::attr(content)').get() or ""
        excerpt = " ".join(excerpt.split()).strip()

        # Fallback: first paragraph-ish
        if not excerpt:
            p = response.css("article p::text, .article p::text, .content p::text").get()
            excerpt = " ".join((p or "").split()).strip()

        yield JptScraperItem(
            source="worldoil",
            url=url,
            title=title,
            excerpt=excerpt,
            published_date=published_date,
            topics=topics,
            tags=tags,  # <-- countries like "Angola" will be here (not in TOPIC_WHITELIST)
            scraped_at=datetime.now(timezone.utc).isoformat(),
            refresh_existing=self.refresh_existing,
        )
