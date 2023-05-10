
import pytest

from sqlalchemy import insert, select, Column, Numeric, Integer, Text, Date
from sqlalchemy.orm import DeclarativeBase

from integration_test_project.quotes.items.models import (
    QuotesBase, Author, Tag, Quote, t_quote_tag
)

from scrapy_sql.utils import *
from scrapy_sql._defaults import _default_insert
from scrapy_sql import ScrapyDeclarativeBase



@pytest.mark.parametrize(
    "input, expected",
    [
        ("    \n\tHello, World!    ", "Hello, World!"),
        ("text          text", "text text"),
    ]
)
def test_normalize_whitespace(input, expected):
    assert normalize_whitespace(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (select(Author.id).where(Author.name == "Fat Albert"), True),
        ('SELECT author.id FROM author WHERE author.name = "Fat Albert"', False),
        (True, False),
        ({}, False)
    ]
)
def test_column_value_is_subquery(input, expected):
    assert column_value_is_subquery(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (Column(Integer), True),
        (Column(Numeric), True),
        (Column(Text), False),
        (Column(Date), False),
    ]
)
def test_is_scalar_column(input, expected):
    assert is_scalar_column(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            select(Author.id).where(Author.name == "Fat Albert"),
            'SELECT author.id FROM author WHERE author.name = "Fat Albert"'
        ),
    ]
)
def test_subquery_to_string(input, expected):
    assert subquery_to_string(input) == expected


# _Load_Table
class LoadTable_DeclarativeBase(DeclarativeBase, ScrapyDeclarativeBase):
    pass


class LoadTable_Model(LoadTable_DeclarativeBase):
    __tablename__ = 'load_table_model'
    id = Column(Integer, primary_key=True)


load_table_table = Table(
    'load_table_table', LoadTable_DeclarativeBase.metadata,
    Column('id', primary_key=True)
)

@pytest.mark.parametrize(
    "obj, table",
    [
        (
            'test_utils.LoadTable_Model',
            LoadTable_Model.__table__
        ),  # String Model
        (
            'test_utils.load_table_table',
            load_table_table
        ),  # String table
        (
            LoadTable_Model,
            LoadTable_Model.__table__
        ),  # Model
        (
            load_table_table,
            load_table_table
        ),  # Table
    ]
)
def test_load_table(obj, table):
    assert load_table(obj) == table


@pytest.mark.parametrize(
    "stmt, expected",
    [
        (
            _default_insert,
            _default_insert
        ),
        (
            'scrapy_sql._defaults._default_insert',
            _default_insert
        )
    ]
)
def test_load_stmt(stmt, expected):
    assert load_stmt(stmt) == expected