
from datetime import datetime
from pathlib import Path

from quotes.items.itemloaders import AuthorLoader, QuoteLoader, TagLoader
from quotes.items.models import Author, Quote, Tag

from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule, Spider


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

            quote_loader = QuoteLoader(selector=quote)
            author_loader = AuthorLoader(selector=quote)

            author_loader.add_xpath('name', './/a[@class="author"]/text()')
            author_instance = author_loader.load_item()

            # There may be a better way to do this...
            # Some way of loading object with subquery for this field 
            quote_loader.add_xpath('quote', './/span[@class="content"]/text()')
            quote_instance = quote_loader.load_item()
            quote_instance.author_id = author_instance.subquery()

            for tag in quote.xpath('.//span[@class="tag"]/text()').getall():
                tag_loader = TagLoader()
                tag_loader.add_value('name', tag)

                quote_instance.tags.append(tag_loader.load_item())

            yield quote_instance

    def parse_author(self, response):
        loader = AuthorLoader(selector=response)

        loader.add_xpath('name',     '//h1/text()')
        loader.add_xpath('birthday', '//span[@class="date"]/text()')
        loader.add_xpath('bio',      '//span[@class="bio"]/text()')

        yield loader.load_item()


class SingleQuoteSpider(Spider):
    """
    Spider that yields a single instance, with relationship
    to see SQLAlchemy Log
    """

    name = 'single_quote'
    allowed_domains = []
    start_urls = ['file://' + str(website_directory / 'index.html')]

    def parse(self, response):

        einstein = Author(**{
            'name': 'Albert Einstein',
            'birthday': datetime(month=3, day=14, year=1879),
            'bio': 'Won the 1921 Nobel Prize in Physics.'
        })

        change = Tag(**{'name': 'change'})
        deep_thoughts = Tag(**{'name': 'deep-thoughts'})

        yield Quote(**{
            'quote': (
                'The world as we have created it is a process of our thinking. '
                'It cannot be changed without changing our thinking.'
            ),
            'author': einstein,
            'tags': [change, deep_thoughts]
        })
