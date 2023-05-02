
import pytest

from scrapy_sql.feedexport import *
from scrapy_sql.exporters import *

def _testing_default_add(session, instance):
    pass

class TestSQLAlchemyInstanceExporter:

    def test_dunder_init(self):
        assert SQLAlchemyInstanceExporter(
            'session_obj', # Doesn't need to be real for this test
            **{'add': _testing_default_add}
        ).add == _testing_default_add

    @pytest.mark.skip(reason="wrapper func")
    def test_export_item(self):
        pass
