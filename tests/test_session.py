
from pprint import pprint
import sqlite3

import pytest

from integration_test_project.quotes.items.models import (
    QuotesBase, Author, Tag, Quote, t_quote_tag
)

from scrapy_sql.session import *
from scrapy_sql.utils import *

from sqlalchemy import create_engine, insert
from sqlalchemy.orm.attributes import instance_state
from sqlalchemy.orm.collections import InstrumentedList

# Relationship are class variables
quote_author_relationship, quote_tags_relationship = instance_state(
    Quote()
).mapper.relationships

# Columns are class variables
quote_id_column = [c for c in Quote.columns if c.name == 'id'][0]
tag_id_column = [c for c in Tag.columns if c.name == 'id'][0]

join_quote_id_column, join_tag_id_column = t_quote_tag.columns


def return_subquery_string_or_arg(arg):
    if column_value_is_subquery(arg):
        return subquery_to_string(arg)
    return arg


def equal_column_values(input, expected):
    return return_subquery_string_or_arg(input) == expected


class TestManyToOneBulkDP:

    @pytest.mark.parametrize(
        "quote_fixture, author_id",
        [
            (
                'simple_quote_with_only_author_id',
                1
            ),
            (
                'simple_quote_with_simple_kennedy_without_id',
                'SELECT author.id FROM author WHERE author.name = "John F. Kennedy"'
            ),
            (
                'simple_quote_with_simple_kennedy_with_id',
                10
            )
        ]
    )
    def test_prepare(self, quote_fixture, author_id, request):

        quote = request.getfixturevalue(quote_fixture)
        ManyToOneBulkDP(quote, quote_author_relationship).prepare()

        assert equal_column_values(quote.author_id, author_id)


class TestManyToManyBulkDP:

    @pytest.mark.parametrize(
        (
            "quote_fixture, parent_instance_fixture, "
            "parent_column, join_table_column, "
            "expected_column_name, expected_column_value"
        ),
        [
            (
                'transient_quote',
                'transient_quote',
                quote_id_column,
                join_quote_id_column,
                'quote_id',
                'SELECT quote.id FROM quote WHERE quote.quote = "If not us, who? If not now, when?"'
            ),
            (
                'transient_quote_with_id',
                'transient_quote_with_id',
                quote_id_column,
                join_quote_id_column,
                'quote_id',
                101
            )
        ]
    )
    def test_determine_join_table_column_value(
        self,
        quote_fixture, parent_instance_fixture,
        parent_column, join_table_column,
        expected_column_name, expected_column_value,
        request
    ):

        # Load fixtures
        quote = request.getfixturevalue(quote_fixture)
        parent_instance = request.getfixturevalue(parent_instance_fixture)

        dp = ManyToManyBulkDP(quote, quote_tags_relationship)

        # returns a dict
        result = dp.determine_join_table_column_value(
            parent_instance,
            parent_column,
            join_table_column
        )
        column_name, column_value = list(result.items())[0]

        assert column_name == expected_column_name
        assert equal_column_values(column_value, expected_column_value)

    def test_prepare_secondary(self, transient_instances):
        kennedy, change, deep_thoughts, quote = transient_instances

        change.id = 123
        quote.tags = InstrumentedList([change, deep_thoughts])
        quote.id = 3

        dp = ManyToManyBulkDP(quote, quote_tags_relationship)
        params = dp.prepare_secondary()

        change, deep_thoughts = params

        assert change == {
            'quote_id': 3,
            'tag_id': 123
        }

        assert deep_thoughts.get('quote_id') == 3
        assert equal_column_values(
            deep_thoughts.get('tag_id'),
            'SELECT tag.id FROM tag WHERE tag.name = "deep-thoughts"'
        )


class TestScrapyBulkSession:

    def test_bulk_commit(self, transient_quote, tmp_path):

        feed_options = {
            'orm_stmts': {
                Author.__table__: insert(Author),
                Tag.__table__:    insert(Tag),
                Quote.__table__:  insert(Quote),
                t_quote_tag:      insert(t_quote_tag),
            },
            'declarative_base': QuotesBase
        }

        # Temp test file
        db_filepath = tmp_path / 'test.db'
        conn_string = f'sqlite:///{db_filepath}'


        engine = create_engine(conn_string)
        QuotesBase.metadata.create_all(engine)

        session = ScrapyBulkSession(bind=engine, feed_options=feed_options)
        session.add(transient_quote)
        session.bulk_commit()

        # verify the records inserted correctly
        conn = sqlite3.connect(db_filepath)
        cur = conn.cursor()

        kennedy = cur.execute('SELECT * FROM author').fetchone()
        change, deep_thoughts = cur.execute('SELECT * FROM tag').fetchall()
        quote = cur.execute('SELECT * FROM quote').fetchone()
        quote_tags = cur.execute('SELECT * FROM quote_tag').fetchall()

        assert kennedy == (
            1,
            'John F. Kennedy',
            '1917-05-29',
            '35th president of the United States.'
        )

        assert change == (
            1,
            'change'
        )

        assert deep_thoughts == (
            2,
            'deep-thoughts'
        )

        assert quote == (
            1,
            1,
            'If not us, who? If not now, when?'
        )

        assert set(quote_tags) == {
            (1, 1),
            (1, 2)
        }
