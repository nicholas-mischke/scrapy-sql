
import pytest

from collections.abc import KeysView
from datetime import date

from integration_test.quotes.items.models import (
    QuotesBase, Author, Tag, Quote, t_quote_tag
)

import sqlalchemy
from sqlalchemy.orm.collections import InstrumentedList

from scrapy_sql.adapters import SQLAlchemyInstanceAdapter, ScrapyDeclarativeBase


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


# class TestScrapyDeclarativeBase:

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_sorted_tables(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_sorted_entities(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_columns(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_column_names(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_column_name_to_column_obj_map(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_unloaded_columns(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_loaded_columns(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_params(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_subquery_from_dict(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_subquery(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_relationships(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_relationship_names(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_relationship_name_to_relationship_obj_map(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_tablename_to_entity_map(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_dunder_repr(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_from_repr(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_under_from_repr_kwargs(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_under_from_repr_columns(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_under_from_repr_subquery(self, input, expected):
#         pass

#     @pytest.mark.parametrize(
#         "input, expected",
#         [
#             (),
#             (),
#             ()
#         ]
#     )
#     def test_under_from_repr_relationships(self, input, expected):
#         pass
