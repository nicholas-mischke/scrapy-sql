
from pathlib import Path
from sqlalchemy import insert

def insert_ignore(table):
    return insert(table).prefix_with('OR IGNORE')

BOT_NAME = 'quotes'

SPIDER_MODULES = ['quotes.spiders']
NEWSPIDER_MODULE = 'quotes.spiders'

ROBOTSTXT_OBEY = False
CLOSESPIDER_ERRORCOUNT = 0

LOG_FILE = Path(__file__).parent.parent / f'{BOT_NAME}.log'

FEED_STORAGES = {
    'mysql':  'scrapy_sql.feedexport.SQLAlchemyFeedStorage',
    'sqlite': 'scrapy_sql.feedexport.SQLAlchemyFeedStorage'
}
FEED_EXPORTERS = {
    'sql': 'scrapy_sql.exporters.SQLAlchemyInstanceExporter'
}
FEEDS = {
    f'sqlite:///{BOT_NAME}.db': {
        'format': 'sql',
        'declarative_base': 'quotes.items.models.QuotesBase',
        'item_filter': 'scrapy_sql.feedexport.SQLAlchemyInstanceFilter',
        'engine_echo': True,
        'orm_stmts': {
            'quotes.items.models.Tag': insert_ignore,
        }
    }
}

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'
TWISTED_REACTOR = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'
