
import pytest

from scrapy_sql._collections import ScrapyTableList


@pytest.fixture
def seeded_session(session):

    with session as session:
        session.add()
        session.commit()
        yield


class TestScrapyTableList:

    def test_append(self):
        pass
