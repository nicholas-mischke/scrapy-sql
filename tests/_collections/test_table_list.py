
import pytest

from scrapy_sql.containers import ScrapyTableList
from tests.data.rows import *


@pytest.fixture
def seeded_session(session):
    session.add(einstein_quote_I)


class TestScrapyTableList:

    def test_append(self):
        pass
