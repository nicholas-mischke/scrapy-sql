
import pytest

from collections.abc import KeysView
from datetime import date, time, datetime

from integration_test_project.quotes.items.models import (
    QuotesBase, Author, Tag, Quote, t_quote_tag
)

import sqlalchemy
from sqlalchemy import select
from sqlalchemy import (
    Column, Date, ForeignKey, Integer, String, Table, Text, Boolean,
    Numeric, Time, DateTime, JSON
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm.collections import InstrumentedList

from scrapy_sql.adapters import SQLAlchemyInstanceAdapter, ScrapyDeclarativeBase
from scrapy_sql.utils import subquery_to_string


# ORM Entities used to test out various from_repr helper methods
class _TestBase(DeclarativeBase, ScrapyDeclarativeBase):
    pass


class FromReprColumns(_TestBase):
    __tablename__ = 'from_repr_columns'

    id = Column(Integer, primary_key=True)  # placeholder primary key

    subquery = Column(Integer)

    none_type = Column(String)  # Could really be any DataType
    boolean = Column(Boolean)

    string = Column(String)
    text = Column(Text)

    integer = Column(Integer)
    numeric = Column(Numeric)

    date = Column(Date)
    time = Column(Time)
    datetime = Column(DateTime)

    json = Column(JSON)


class TestSQLAlchemyInstanceAdapter:

    # _MixinColumnSQLAlchemyAdapter
    @pytest.mark.parametrize(
        "input, expected",
        [
            (Author(), {
                'id': None,
                'name': None,
                'birthday': None,
                'bio': None
            }),
            (Tag(), {
                'id': None,
                'name': None
            }),
            (Quote(), {
                'id': None,
                'author_id': None,
                'quote': None,
                'author': None,
                'tags': InstrumentedList()
            })
        ]
    )
    def test_under_fields_dict(self, input, expected):
        adapter = SQLAlchemyInstanceAdapter(input)
        assert adapter._fields_dict == expected

    def test_under_fields_dict_persistent(self, persistent_instances):
        kennedy, change, deep_thoughts, quote = persistent_instances
        adapter = SQLAlchemyInstanceAdapter(quote)

        assert adapter._fields_dict == {
            'id': 1,
            'author_id': 1,
            'quote': 'If not us, who? If not now, when?',
            'author': kennedy,
            'tags': InstrumentedList([change, deep_thoughts])
        }

    def test_dunder_delitem(self, transient_instances):
        """
        Should set all Columns back to NoneType
        Should set all Relationships back to NoneType or an empty InstrumentedList
        """
        kennedy, change, deep_thoughts, quote = transient_instances
        adapter = SQLAlchemyInstanceAdapter(quote)

        # Column back to NoneType
        assert quote.quote == 'If not us, who? If not now, when?'
        del adapter['quote']
        assert quote.quote == None

        assert quote.author == kennedy
        del adapter['author']
        assert quote.author == None

        assert quote.tags == InstrumentedList([change, deep_thoughts])
        del adapter['tags']
        assert quote.tags == InstrumentedList()

    def test_dunder_getitem(self, persistent_instances):
        kennedy, change, deep_thoughts, quote = persistent_instances
        adapter = SQLAlchemyInstanceAdapter(quote)

        assert adapter['id'] == 1
        assert adapter['author_id'] == 1
        assert adapter['quote'] == 'If not us, who? If not now, when?'
        assert adapter['author'] == kennedy
        assert adapter['tags'] == InstrumentedList([change, deep_thoughts])

    def test_dunder_setitem(self):
        author = Author()
        adapter = SQLAlchemyInstanceAdapter(author)

        adapter['id'] = 1
        adapter['name'] = 'Albert Einstein'
        adapter['birthday'] = date(month=3, day=14, year=1879)
        adapter['bio'] = 'Won the 1921 Nobel Prize in Physics.'

        assert author.id == 1
        assert author.name == 'Albert Einstein'
        assert author.birthday == date(month=3, day=14, year=1879)
        assert author.bio == 'Won the 1921 Nobel Prize in Physics.'

    def test_dunder_setitem_exception(self):
        author = Author()
        adapter = SQLAlchemyInstanceAdapter(author)

        with pytest.raises(KeyError):
            adapter['nonexistent_attr'] = 1

    def test_dunder_iter(self):
        adapter = SQLAlchemyInstanceAdapter(Tag())

        attrs = ('id', 'name')
        for attr, value in adapter.items():
            assert attr in attrs

    @pytest.mark.parametrize(
        "input, expected",
        [
            (Author(), 4),
            (Tag(), 2),
            (Quote(), 5)
        ]
    )
    def test_dunder_len(self, input, expected):
        adapter = SQLAlchemyInstanceAdapter(input)
        assert len(adapter) == expected

    # SQLAlchemyInstanceAdapter
    @pytest.mark.parametrize(
        "input, expected",
        [
            (Author(), True),
            (Tag(), True),
            (Quote(), True),
            (t_quote_tag, False),
            ({}, False)
        ]
    )
    def test_is_item(self, input, expected):
        assert SQLAlchemyInstanceAdapter.is_item(input) == expected

    @pytest.mark.parametrize(
        "input, expected",
        [
            (QuotesBase, True),
            (Author, True),
            (Tag, True),
            (Quote, True),
            (sqlalchemy.Table, False),
            (dict, False)
        ]
    )
    def test_is_item_class(self, input, expected):
        assert SQLAlchemyInstanceAdapter.is_item_class(input) == expected

    @pytest.mark.parametrize(
        "input, expected",
        [
            (Author, ['id', 'name', 'birthday', 'bio']),
            (Tag, ['id', 'name']),
            (Quote, ['id', 'author_id', 'quote', 'author', 'tags'])
        ]
    )
    def test_get_field_names_from_class(self, input, expected):
        assert SQLAlchemyInstanceAdapter.get_field_names_from_class(
            input
        ) == expected

    @pytest.mark.parametrize(
        "instance",
        [
            (Author()),
            (Tag()),
            (Quote())
        ]
    )
    def test_dunder_init(self, instance):
        adapter = SQLAlchemyInstanceAdapter(instance)
        assert adapter.item == instance

    def test_asdict(self, persistent_instances):
        adapter = SQLAlchemyInstanceAdapter(Quote())
        assert adapter.asdict() == {
            'id': None,
            'author_id': None,
            'quote': None,
            'author': None,
            'tags': InstrumentedList()
        }

        kennedy, change, deep_thoughts, quote = persistent_instances
        adapter = SQLAlchemyInstanceAdapter(quote)
        assert adapter.asdict() == {
            'id': 1,
            'author_id': 1,
            'quote': 'If not us, who? If not now, when?',
            'author': kennedy,
            'tags': InstrumentedList([change, deep_thoughts])
        }

    @pytest.mark.parametrize(
        "instance, expected",
        [
            (Author(), ['id', 'name', 'birthday', 'bio']),
            (Tag(), ['id', 'name']),
            (Quote(), ['id', 'author_id', 'quote', 'author', 'tags'])
        ]
    )
    def test_field_names(self, instance, expected):
        assert SQLAlchemyInstanceAdapter(instance).field_names() \
            == KeysView(expected)


class TestScrapyDeclarativeBase:

    def test_sorted_tables(self):
        assert Quote.sorted_tables == [
            Author.__table__,
            Tag.__table__,
            Quote.__table__,
            t_quote_tag
        ]

    def test_sorted_entities(self):
        assert Quote.sorted_entities == [
            Author,
            Tag,
            Quote
        ]

    def test_tablename_to_entity_map(self):
        assert Quote.tablename_to_entity_map == {
            'author': Author,
            'tag': Tag,
            'quote': Quote,
            'quote_tag': t_quote_tag
        }

    @pytest.mark.skip(reason="wrapper property")
    def test_columns(self):
        pass

    @pytest.mark.parametrize(
        "instance, expected",
        [
            (Author(), ('id', 'name', 'birthday', 'bio')),
            (Tag(), ('id', 'name')),
            (Quote(), ('id', 'author_id', 'quote')),
        ]
    )
    def test_column_names(self, instance, expected):
        assert instance.column_names == expected

    @pytest.mark.skip(reason="dictionary comprehension of wrapper property")
    def test_column_name_to_column_obj_map(self):
        pass

    @pytest.mark.skip(reason="wrapper property")
    def test_relationships(self):
        pass

    @pytest.mark.parametrize(
        "input, expected",
        [
            (Author, tuple()),
            (Quote, ('author', 'tags'))
        ]
    )
    def test_relationship_names(self, input, expected):
        assert input.relationship_names == expected

    @pytest.mark.skip(reason="dictionary comprehension of wrapper property")
    def test_relationship_name_to_relationship_obj_map(self):
        pass

    def test_unloaded_columns_empty(self, empty_quote):
        unloaded_columns = empty_quote.unloaded_columns
        assert tuple(c.name for c in unloaded_columns) \
            == ('id', 'author_id', 'quote')

    def test_unloaded_columns_pending(self, pending_quote):
        unloaded_columns = pending_quote.unloaded_columns
        assert tuple(c.name for c in unloaded_columns) \
            == ('id', 'author_id')

    def test_unloaded_columns_persistent(self, persistent_quote):
        assert persistent_quote.unloaded_columns == tuple()

    def test_loaded_columns_empty(self, empty_quote):
        assert empty_quote.loaded_columns == tuple()

    def test_loaded_columns_pending(self, pending_quote):
        loaded_columns = pending_quote.loaded_columns
        assert tuple(c.name for c in loaded_columns) \
            == ('quote', )

    def test_loaded_columns_persistent(self, persistent_quote):
        loaded_columns = persistent_quote.loaded_columns
        assert tuple(c.name for c in loaded_columns) \
            == ('id', 'author_id', 'quote')

    def test_params(self, pending_quote):
        assert pending_quote.params == {
            'quote': 'If not us, who? If not now, when?'
        }

    @pytest.mark.parametrize(
        "cls, return_columns, instance_kwargs, subquery_string",
        [
            (
                Author,
                tuple(),
                {'name': 'Fat Albert'},
                'SELECT author.id FROM author WHERE author.name = "Fat Albert"'
            ),
            (
                Author,
                ('birthday', ),
                {'name': 'Fat Albert'},
                'SELECT author.birthday FROM author WHERE author.name = "Fat Albert"'
            ),
            (
                Author,
                ('id', 'birthday'),
                {'name': 'Fat Albert'},
                'SELECT author.id, author.birthday FROM author WHERE author.name = "Fat Albert"'
            ),
        ]
    )
    def test_subquery_from_dict(self, cls, return_columns, instance_kwargs, subquery_string):
        assert subquery_to_string(cls.subquery_from_dict(
            *return_columns, **instance_kwargs
        )) == subquery_string

    def test_subquery(self, pending_instances):
        """Is really both a test for subquery method and utils.subquery_to_string func"""
        kennedy, change, deep_thoughts, quote = pending_instances

        assert subquery_to_string(kennedy.subquery()) == (
            'SELECT author.id FROM author '
            'WHERE author.name = "John F. Kennedy" '
            'AND author.bio = "35th president of the United States."'
        )

        assert subquery_to_string(change.subquery()) == (
            'SELECT tag.id FROM tag WHERE tag.name = "change"'
        )

        assert subquery_to_string(quote.subquery()) == (
            'SELECT quote.id FROM quote WHERE quote.quote = "If not us, who? If not now, when?"'
        )

    def test_dunder_repr(self, persistent_instances):
        kennedy, change, deep_thoughts, quote = persistent_instances

        assert repr(kennedy) == \
            'Author(id=1, name="John F. Kennedy", birthday=1917-05-29, bio="35th president of the United States.")'

        assert repr(change) == \
            'Tag(id=1, name="change")'

        assert repr(quote) == (
            'Quote(id=1, author_id=1, '
            'quote="If not us, who? If not now, when?", '
            'author=Author(id=1, name="John F. Kennedy", birthday=1917-05-29, bio="35th president of the United States."), '
            'tags=[Tag(id=1, name="change"), Tag(id=2, name="deep-thoughts")])'
        )

    def test_from_repr(self):
        quote_string = (
            'Quote(id=1, author_id=1, '
            'quote="If not us, who? If not now, when?", '
            'author=Author(id=1, name="John F. Kennedy", birthday=1917-05-29, bio="35th president of the United States."), '
            'tags=[Tag(id=1, name="change"), Tag(id=2, name="deep-thoughts")])'
        )
        quote = Quote.from_repr(quote_string)

        assert quote.id == 1
        assert quote.author_id == 1
        assert quote.quote == "If not us, who? If not now, when?"

        kennedy = quote.author
        assert kennedy.id == 1
        assert kennedy.name == "John F. Kennedy"
        assert kennedy.birthday == date(day=29, month=5, year=1917)
        assert kennedy.bio == "35th president of the United States."

        change, deep_thoughts = quote.tags  # Should be ordered, I think...
        assert change.id == 1
        assert change.name == "change"

        assert deep_thoughts.id == 2
        assert deep_thoughts.name == "deep-thoughts"

    def test_under_from_repr_kwargs(self):
        kennedy_repr = 'Author(id=1, name="John F. Kennedy", birthday=1917-05-29, bio="35th president of the United States.")'
        change_repr = 'Tag(id=1, name="change")'
        deep_thoughts_repr = 'Tag(id=2, name="deep-thoughts")'
        quote_repr = (
            'Quote(id=1, author_id=1, '
            'quote="If not us, who? If not now, when?", '
            'author=Author(id=1, name="John F. Kennedy", birthday=1917-05-29, bio="35th president of the United States."), '
            'tags=[Tag(id=1, name="change"), Tag(id=2, name="deep-thoughts")])'
        )

        assert Author._from_repr_kwargs(kennedy_repr) == {
            'id': '1',
            'name': 'John F. Kennedy',
            'birthday': '1917-05-29',
            'bio': '35th president of the United States.'
        }
        assert Tag._from_repr_kwargs(change_repr) == {
            'id': '1',
            'name': 'change'
        }
        assert Tag._from_repr_kwargs(deep_thoughts_repr) == {
            'id': '2',
            'name': 'deep-thoughts'
        }
        assert Quote._from_repr_kwargs(quote_repr) == {
            'id': '1',
            'author_id': '1',
            'quote': 'If not us, who? If not now, when?',
            'author': kennedy_repr,
            'tags': f"[{change_repr}, {deep_thoughts_repr}]"
        }

    def test_under_from_repr_columns(self):
        columns = FromReprColumns.columns
        values = (
            '1',  # id (also an Integer)
            'SELECT from_repr_columns.id FROM from_repr_columns WHERE text = "test string"',  # subquery
            'None',  # none_type value for any Column DataType
            'False',  # Boolean
            'test string',  # String
            'test text',  # Text
            '10',  # Integer
            '1.25',  # Numeric
            '2000-01-01',  # Date
            '10:30:15',  # Time
            '2000-01-01 10:30:15',  # DateTime
            '{"key_1": 1, "key_2": 2}'  # JSON
        )
        columns_and_strings = dict(zip(columns, values))

        results = {}
        for column, string in columns_and_strings.items():
            results[column.name] = FromReprColumns._from_repr_columns(
                column,
                string
            )

        assert results['subquery'].is_clause_element
        results['subquery'] = subquery_to_string(results['subquery'])

        assert results == {
            'id': 1,
            'subquery': 'SELECT from_repr_columns.id FROM from_repr_columns WHERE text = "test string"',

            'none_type': None,
            'boolean': False,

            'string': 'test string',
            'text': 'test text',

            'integer': 10,
            'numeric': 1.25,

            'date': date(day=1, month=1, year=2000),
            'time': time(hour=10, minute=30, second=15),
            'datetime': datetime(day=1, month=1, year=2000, hour=10, minute=30, second=15),

            'json': {"key_1": 1, "key_2": 2}
        }

    def test_under_from_repr_subquery(self):
        subquery_string = 'SELECT author.id FROM author WHERE author.name = "Fed"'
        subquery = Author._from_repr_subquery(None, subquery_string)

        assert subquery.is_clause_element
        assert subquery.text == subquery_string

    def test_under_from_repr_relationships(self):
        relationships = Quote.relationships
        values = (
            'Author(id=1, name="John F. Kennedy", birthday=1917-05-29, bio="35th president of the United States.")',
            '[Tag(id=1, name="change"), Tag(id=2, name="deep-thoughts")])'
        )
        relationships_and_strings = dict(zip(relationships, values))

        results = {}
        for relationship, string in relationships_and_strings.items():
            results[relationship.class_attribute.key] = Quote._from_repr_relationships(
                relationship, string
            )

        assert isinstance(results['author'], Author)
        assert isinstance(results['tags'], InstrumentedList)
        for instance in results['tags']:
            assert isinstance(instance, Tag)

        author = results['author']
        assert author.id == 1
        assert author.name == "John F. Kennedy"
        assert author.birthday == date(day=29, month=5, year=1917)
        assert author.bio == "35th president of the United States."

        change, deep_thoughts = results['tags']

        assert change.id == 1
        assert change.name == 'change'

        assert deep_thoughts.id == 2
        assert deep_thoughts.name == 'deep-thoughts'

        # Empty Values
        values = ('None', '[]')
        relationships_and_strings = dict(zip(relationships, values))

        results = {}
        for relationship, string in relationships_and_strings.items():
            results[relationship.class_attribute.key] = Quote._from_repr_relationships(
                relationship, string
            )

        assert results['author'] == None
        assert results['tags'] == InstrumentedList()


