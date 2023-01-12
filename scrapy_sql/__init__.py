
# Declare top-level shortcuts
from .tableadapter import SQLAlchemyTableAdapter, ScrapyDeclarativeMetaAdapter
from .exporters import SQLAlchemyTableExporter
from .feedexport import SQLAlchemyFeedStorage, SQLAlchemySQLiteFeedStorage
from .sessions import databases_info

__all__ = [
    'ScrapyDeclarativeMetaAdapter',
    'SQLAlchemyTableExporter',
    'SQLAlchemyFeedStorage',
    'SQLAlchemySQLiteFeedStorage',
    'databases_info',
]

# Update scrapy ItemAdapter class to work with SQLAlchemy.
import itemadapter
itemadapter.adapter.ItemAdapter.ADAPTER_CLASSES.appendleft(
    SQLAlchemyTableAdapter
)
