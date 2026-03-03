import scrapy

class OilPriceSpider(scrapy.Spider):
    name = "oilprice_company_news"
    allowed_domains = ["oilprice.com"]
    start_urls = ["https://oilprice.com/Company-News/"]

    custom_settings = {
        "CONCURRENT_REQUESTS": 12,
        "AUTOTHROTTLE_ENABLED": True,
        "DOWNLOAD_DELAY": 0.5,
        "ROBOTSTXT_OBEY": True,
        "LOG_LEVEL": "INFO",
    }

    def parse(self, response):
        for article in response.css("div.categoryArticle"):
            company_name = article.css("p.categoryArticle__companyName::text").get()
            title = article.css("h2.categoryArticle__title::text").get()
            link = article.css("a::attr(href)").get()
            excerpt = article.css("p.categoryArticle__excerpt::text").get()
            meta_info = article.css("p.categoryArticle__meta::text").get()

            if company_name:
                yield {
                    "company_name": company_name.strip(),
                    "title": title.strip() if title else None,
                    "url": response.urljoin(link) if link else None,
                    "excerpt": excerpt.strip() if excerpt else None,
                    "meta_info": meta_info.strip() if meta_info else None,
                }

        # Follow pagination
        next_page = response.css("div.pagination a.next::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
