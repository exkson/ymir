import re

import scrapy


class EmailSpider(scrapy.Spider):
    SEARCH_URL = "https://www.youtube.com/results"
    CHANNEL_PATTERN = re.compile(r'"canonicalBaseUrl":\s?"\/(@[^"]+)"')
    EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
    EMAILS_BLACKLIST = [
        "wght@300..900",
    ]

    name = "email"

    def start_requests(self):
        for query in self.get_queries():
            yield scrapy.Request(
                self.SEARCH_URL + "?search_query=" + query,
                self.parse_search_results,
                cb_kwargs={"query": query},
            )

    def parse_search_results(self, response, **kwargs):
        channels = set(self.CHANNEL_PATTERN.findall(response.text))

        for channel in channels:
            yield scrapy.Request(
                f"https://www.youtube.com/{channel}",
                self.parse_channel,
                cb_kwargs={"channel": channel, "query": kwargs["query"]},
            )

    def parse_channel(self, response, **kwargs):
        email_addresses = set(self.EMAIL_PATTERN.findall(response.text))
        for email in email_addresses:
            if email in self.EMAILS_BLACKLIST:
                continue
            yield {
                "email": email,
                "channel": kwargs["channel"],
                "query": kwargs["query"],
            }

    def get_queries(self):
        with open("datasets/queries.txt") as f:
            queries = f.readlines()
        yield from queries
