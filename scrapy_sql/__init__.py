
# Declare top-level shortcuts
from pathlib import Path
from .tableadapter import SQLAlchemyTableAdapter, ScrapyDeclarativeMetaAdapter, QueryFilter
from .feedexport import SQLAlchemyFeedStorage
from .exporters import SQLAlchemyTableExporter

__all__ = [
    'QueryFilter'
    'ScrapyDeclarativeMetaAdapter',
    'SQLAlchemyFeedStorage',
    'SQLAlchemyTableExporter',
]

# Update scrapy ItemAdapter class to work with SQLAlchemy.
import itemadapter
itemadapter.adapter.ItemAdapter.ADAPTER_CLASSES.appendleft(
    SQLAlchemyTableAdapter
)


# This should be deleted soon....
db_file = Path(__file__).parent.parent / 'tests/integration_test/quotes.db'
log_file = Path(__file__).parent.parent / 'tests/integration_test/quotes.log'

db_file.unlink(missing_ok=True)
log_file.unlink(missing_ok=True)
