
# Declare top-level shortcuts
from .tableadapter import SQLAlchemyTableAdapter, ScrapyDeclarativeMetaAdapter
from .feedexport import SQLAlchemyFeedStorage
from .exporters import SQLAlchemyTableExporter
from .connections import connection_info


__all__ = [
    'ScrapyDeclarativeMetaAdapter',
    'SQLAlchemyFeedStorage',
    'SQLAlchemyTableExporter',
    'connection_info'
]

# Update scrapy ItemAdapter class to work with SQLAlchemy.
import itemadapter
itemadapter.adapter.ItemAdapter.ADAPTER_CLASSES.appendleft(
    SQLAlchemyTableAdapter
)


# This should be deleted soon....
from pathlib import Path
db_file  = Path(__file__).parent.parent / 'tests/integration_test/quotes.db'
log_file = Path(__file__).parent.parent / 'tests/integration_test/quotes.log'

db_file.unlink(missing_ok=True)
log_file.unlink(missing_ok=True)
