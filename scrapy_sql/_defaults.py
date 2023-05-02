
from scrapy_sql.session import ScrapyBulkSession
from sqlalchemy import insert


def _default_add(session, instance):
    session.add(instance)


def _default_commit(session):
    if isinstance(session, ScrapyBulkSession):
        session.bulk_commit()
    else:
        session.commit()


def _default_insert(table):
    return insert(table)
