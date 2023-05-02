
import pytest

from sqlalchemy import select, Column, Numeric, Integer, Text, Date

from integration_test_project.quotes.items.models import (
    QuotesBase, Author, Tag, Quote, t_quote_tag
)
from scrapy_sql.utils import *


@pytest.mark.parametrize(
    "input, expected",
    [
        ("    \n\tHello, World!    ", "Hello, World!"),
        ("text          text", "text text"),
    ]
)
def test_clean_text(input, expected):
    assert clean_text(input) == expected


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
