
from pathlib import Path
import math

BOT_NAME = 'quotes'

SPIDER_MODULES = ['quotes.spiders']
NEWSPIDER_MODULE = 'quotes.spiders'

ROBOTSTXT_OBEY = False
CLOSESPIDER_ERRORCOUNT = 0
LOG_FILE = Path(__file__).parent.parent / 'quotes.log'

FEED_STORAGES = {
    'mysql':  'scrapy_sql.feedexport.SQLAlchemyFeedStorage',
    'sqlite': 'scrapy_sql.feedexport.SQLAlchemyFeedStorage'
}
FEED_EXPORTERS = {
    'sql': 'scrapy_sql.exporters.SQLAlchemyTableExporter'
}
FEEDS = {
    'sqlite:///quotes.db': {
        'format': 'sql',
        'item_classes': (
            'quotes.models.Author',
            'quotes.models.Quote',
            'quotes.models.Tag',
            'quotes.models.t_quote_tag'
        )
    }
}

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'
TWISTED_REACTOR = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'
