
from pathlib import Path

from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from quotes.itemloaders import QuoteLoader, AuthorLoader, TagLoader


website_directory = Path(__file__).parent.parent.parent / 'quote_website'


class QuotesSpider(CrawlSpider):
    name = 'quotes'
    allowed_domains = []
    start_urls = ['file://' + str(website_directory / 'index.html')]

    rules = (
        Rule(
            link_extractor=LinkExtractor(allow=(r"page_\d")),
            callback='parse_quotes'
        ),
        Rule(
            link_extractor=LinkExtractor(allow=(r"\/authors\/")),
            callback='parse_author'
        )
    )

    def parse_quotes(self, response):

        for quote in response.xpath('//div[@class="quote"]'):
            loader = QuoteLoader(selector=quote)

            loader.add_xpath('quote',  './/span[@class="content"]/text()')
            loader.add_xpath('authorDOTname', './/a[@class="author"]/text()')

            quote_row = loader.load_item()

            for tag in quote.xpath('.//span[@class="tag"]/text()').getall():
                loader = TagLoader()
                loader.add_value('name', tag)
                quote_row.tags.append(loader.load_item())

            yield quote_row

    def parse_author(self, response):
        loader = AuthorLoader(selector=response)

        loader.add_xpath('name',     '//h1/text()')
        loader.add_xpath('birthday', '//span[@class="date"]/text()')
        loader.add_xpath('bio',      '//span[@class="bio"]/text()')

        yield loader.load_item()
