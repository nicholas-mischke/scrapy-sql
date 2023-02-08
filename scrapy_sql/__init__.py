
# Declare top-level shortcuts
from .declarative_base import ScrapyDeclarativeBase
from .exporters import SQLAlchemyInstanceExporter
from .feedexport import SQLAlchemyFeedStorage

__all__ = [
    'ScrapyDeclarativeBase',
    'SQLAlchemyInstanceExporter',
    'SQLAlchemyFeedStorage'
]

# Update scrapy ItemAdapter class to work with SQLAlchemy.
from .instanceadapter import SQLAlchemyInstanceAdapter
import itemadapter

itemadapter.adapter.ItemAdapter.ADAPTER_CLASSES.appendleft(
    SQLAlchemyInstanceAdapter
)
