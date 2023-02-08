
# Declare top-level shortcuts
from .declarative_base import ScrapyDeclarativeBase
from .exporters import SQLAlchemyInstanceExporter
from .feedexport import SQLAlchemyFeedStorage
from .instanceadapter import SQLAlchemyInstanceAdapter


__all__ = [
    'ScrapyDeclarativeBase',
    'SQLAlchemyInstanceExporter',
    'SQLAlchemyFeedStorage',
    'SQLAlchemyInstanceAdapter'
]

# Update scrapy ItemAdapter class to work with SQLAlchemy.
import itemadapter
itemadapter.adapter.ItemAdapter.ADAPTER_CLASSES.appendleft(
    SQLAlchemyInstanceAdapter
)
