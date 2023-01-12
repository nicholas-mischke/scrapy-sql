
from scrapy_sql.loader import *


from scrapy_sql.exporters import SQLAlchemyTableExporter
import pytest
from datetime import datetime
from pathlib import Path

from sqlalchemy import (
    create_engine,
    Column, DateTime, ForeignKey,
    Integer, String, Table, Text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.pool import NullPool

from scrapy_sql import ScrapyDeclarativeMetaAdapter, SQLAlchemyTableAdapter
from scrapy_sql.loader import DATE_TYPES, TableLoader
import scrapy_sql

from itemloaders.processors import Identity, TakeFirst


# Need Table, TableLoader & Session objects for these tests
engine = create_engine('sqlite:///:memory:')  # , poolclass=NullPool)
Base = declarative_base()
metadata = Base.metadata

Session = sessionmaker()
Session.configure(bind=engine)
session = Session()


class Table_I(Base, ScrapyDeclarativeMetaAdapter):
    __tablename__ = 'table_I'

    id = Column(Integer, primary_key=True, autoincrement=True)
    string = Column(String(50), unique=True)
    date = Column(DateTime)


class Tag(Base, ScrapyDeclarativeMetaAdapter):
    __tablename__ = 'tag'

    id = Column(Integer, primary_key=True)
    name = Column(String(31), unique=True)


metadata.create_all(engine)

################################################################################


class TableLoader_I(TableLoader):
    default_item_class = Table_I

    default_input_processor = Identity()
    default_output_processor = TakeFirst()


def test_load_table_I(mocker):
    # this mock works...
    mocker.patch.object(
        scrapy_sql.loader,
        'session',
        session
    )

    loader_I = TableLoader_I()
    loader_I.add_value('string', 'test_string')
    loader_I.add_value('date', datetime(year=2000, month=1, day=1))
    table_I = loader_I.load_table()

    session.add(table_I)

    loader_II = TableLoader_I()
    loader_II.add_value('string', 'test_string')
    loader_II.add_value('date', datetime(year=2000, month=1, day=1))
    table_II = loader_II.load_table()

    assert table_I == table_II


class TagLoader(TableLoader):
    default_item_class = Tag

    default_input_processor = Identity()
    default_output_processor = TakeFirst()


def test_load_tag_table(mocker):

    # this mock works...
    mocker.patch.object(
        scrapy_sql.loader,
        'session',
        session
    )

    loader_I = TagLoader()
    loader_I.add_value('name', 'change')
    table_I = loader_I.load_table()

    session.add(table_I)

    loader_II = TagLoader()
    loader_II.add_value('name', 'change')
    table_II = loader_II.load_table()

    assert table_I == table_II


def test_under_add():
    pass
