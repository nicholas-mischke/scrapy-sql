
# Declare top-level shortcuts
from .adapters import ScrapyDeclarativeBase
from .exporters import SQLAlchemyInstanceExporter
from .feedexport import SQLAlchemyFeedStorage

__all__ = [
    'ScrapyDeclarativeBase',
    'SQLAlchemyInstanceExporter',
    'SQLAlchemyFeedStorage'
]

# Update scrapy ItemAdapter class to work with SQLAlchemy.
from .adapters import SQLAlchemyInstanceAdapter
import itemadapter

itemadapter.adapter.ItemAdapter.ADAPTER_CLASSES.appendleft(
    SQLAlchemyInstanceAdapter
)
