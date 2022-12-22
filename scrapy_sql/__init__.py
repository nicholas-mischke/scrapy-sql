
# Declare top-level shortcuts
from .tableadapter import SQLAlchemyTableAdapter, ScrapyDeclarativeMetaAdapter
from .exporters import SQLAlchemyTableExporter
from .feedexport import SQLAlchemyFeedStorage, SQLAlchemySQLiteFeedStorage
from .loader import TableLoader
from .sessions import databases_info, session

__all__ = [
    # tableadapter
    'ScrapyDeclarativeMetaAdapter',
    'SQLAlchemyTableExporter',

    #
    'SQLAlchemyFeedStorage',
    'SQLAlchemySQLiteFeedStorage',
    'TableLoader',
    'databases_info',
    'session'
]

# Update scrapy ItemAdapter class to work with SQLAlchemy
import itemadapter
itemadapter.adapter.ItemAdapter.ADAPTER_CLASSES.appendleft(
    SQLAlchemyTableAdapter
)
