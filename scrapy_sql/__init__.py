
# Declare top-level shortcuts
from .exporters import SQLAlchemyTableExporter
from .feedexport import SQLAlchemyFeedStorage
from .tableadapter import SQLAlchemyTableAdapter, ScrapyDeclarativeMetaAdapter


__all__ = [
    'SQLAlchemyTableExporter',
    'SQLAlchemyFeedStorage',
    'ScrapyDeclarativeMetaAdapter'
]

# Update scrapy ItemAdapter class to work with SQLAlchemy.
import itemadapter
itemadapter.adapter.ItemAdapter.ADAPTER_CLASSES.appendleft(
    SQLAlchemyTableAdapter
)
