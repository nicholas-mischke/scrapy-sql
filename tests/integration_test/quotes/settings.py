
from pathlib import Path

FILE_NAMES = 'quotes'

BOT_NAME = 'quotes'

SPIDER_MODULES = ['quotes.spiders']
NEWSPIDER_MODULE = 'quotes.spiders'

ROBOTSTXT_OBEY = False
CLOSESPIDER_ERRORCOUNT = 0

LOG_FORMAT = '%(asctime)s file: %(filename)s line: %(lineno)d [%(name)s] %(levelname)s: %(message)s'
# LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_FILE = Path(__file__).parent.parent / f'{FILE_NAMES}.log'

FEED_STORAGES = {
    'mysql':  'scrapy_sql.feedexport.SQLAlchemyFeedStorage',
    'sqlite': 'scrapy_sql.feedexport.SQLAlchemyFeedStorage'
}
FEED_EXPORTERS = {
    'sql': 'scrapy_sql.exporters.SQLAlchemyInstanceExporter'
}
FEEDS = {
    f'sqlite:///{FILE_NAMES}.db': {
        'format': 'sql',
        'declarative_base': 'quotes.items.models.QuotesBase',
        'orm_stmts': {
            'quotes.items.models.Author':      'scrapy_sql.utils.insert_ignore',
            'quotes.items.models.Tag':         'scrapy_sql.utils.insert_ignore',
            'quotes.items.models.Quote':       'scrapy_sql.utils.insert_ignore',
            'quotes.items.models.t_quote_tag': 'scrapy_sql.utils.insert_ignore'
        },
        'item_classes': (
            'quotes.items.models.Author',
            'quotes.items.models.Tag',
            'quotes.items.models.Quote'
        )
    }
}

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'
TWISTED_REACTOR = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'
