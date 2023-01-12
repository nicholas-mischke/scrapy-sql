
import pytest
import datetime

from sqlalchemy import (
    create_engine,
    Column, Date, DateTime, Time, TIMESTAMP, ForeignKey,
    Integer, String, Table, Text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


from scrapy_sql import ScrapyDeclarativeMetaAdapter, SQLAlchemyTableAdapter


@pytest.fixture(scope='session')
def engine():
    return create_engine(conn_str)


@pytest.fixture(scope='session')
def base():
    return declarative_base()


@pytest.fixture(scope='function')
def session(engine):
    return sessionmaker().configure(bind=engine)()


@pytest.fixture(scope='function')
def generic_table():
    class GenericTable(declarative_base(), ScrapyDeclarativeMetaAdapter):
        __tablename__ = 'generic_table'

        id = Column(Integer, primary_key=True)
        string_column = Column(String(50), unique=True)

        date_column = Column(Date)
        datetime_column = Column(DateTime)
        time_column = Column(Time)
        timestamp_column = Column(TIMESTAMP)

    obj = GenericTable()

    obj.string_column = 'test_string'

    date_column = datetime.date(year=1_000, month=1, day=1)
    datetime_column = datetime.datetime(
        year=1_000,
        month=1,
        day=1,
        hour=1,
        minute=1,
        second=1,
        microsecond=1,
    )
    time_column = datetime.time(hour=1, minute=1, second=1, microsecond=1)
    timestamp_column = datetime.time(hour=1, minute=1, second=1, microsecond=1)

    return obj


@pytest.fixture
def generic_table_adapted(generic_table):
    return SQLAlchemyTableAdapter(generic_table)
