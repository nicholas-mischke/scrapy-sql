
# PyTest & PyTest Fixture Imports
import pytest
from data import *

# Package Imports
from scrapy_sql.tableadapter import Relationship, SQLAlchemyTableAdapter

# SQLAlchemy Imports
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.orm.decl_api import DeclarativeMeta

# 3rd 🎉 Imports
from collections.abc import KeysView
from datetime import datetime

new_author = Author()
new_tag = Tag()
new_quote = Quote()


class TestSQLAlchemyTableAdapter:

    ############################ AdapterInterface ##############################
    @pytest.mark.parametrize(
        "instance, expected",
        [
            (new_author, {
                'item': new_author,
                'item_class': Author,
                'item_class_name': 'Author'
            }),
            (new_tag, {
                'item': new_tag,
                'item_class': Tag,
                'item_class_name': 'Tag'
            }),
            (new_quote, {
                'item': new_quote,
                'item_class': Quote,
                'item_class_name': 'Quote'
            })
        ]
    )
    def test_dunder_init(self, instance, expected):
        assert SQLAlchemyTableAdapter(instance).__dict__ == expected

    @pytest.mark.parametrize(
        "instance, expected",
        [
            (TestTable(),     True),
            (new_author, True),
            (new_tag,    True),
            (new_quote,  True),

            ({}, False),
            (Base.metadata, False),
            (Author.__table__, False),
            (t_quote_tag, False)
        ]
    )
    def test_is_item(self, instance, expected):
        assert SQLAlchemyTableAdapter.is_item(instance) == expected

    @pytest.mark.parametrize(
        "cls, expected",
        [
            (Base,   True),
            (Author, True),
            (Tag,    True),
            (Quote,  True),

            (dict, False),
            (Author.__table__, False),
            (t_quote_tag, False)
        ]
    )
    def test_is_item_class(self, cls, expected):
        assert SQLAlchemyTableAdapter.is_item_class(cls) == expected

    @pytest.mark.parametrize(
        "cls, expected",
        [
            (Author, ['id', 'name', 'birthday', 'bio']),
            (Tag,    ['id', 'name']),
            (Quote,  ['id', 'author_id', 'quote']),
        ]
    )
    def test_get_field_names_from_class(self, cls, expected):
        assert SQLAlchemyTableAdapter.get_field_names_from_class(
            cls) == expected

    @pytest.mark.parametrize(
        "instance, expected",
        [
            (new_author, KeysView(['id', 'name', 'birthday', 'bio'])),
            (new_tag,    KeysView(['id', 'name'])),
            (new_quote,  KeysView(['id', 'author_id', 'quote'])),
        ]
    )
    def test_field_names(self, instance, expected):
        assert SQLAlchemyTableAdapter(instance).field_names() == expected

    @pytest.mark.parametrize(
        "instance, expected",
        [
            (new_author, {
                'id': None,
                'name': None,
                'birthday': None,
                'bio': None
            }),
            (new_tag,    {'id': None, 'name': None}),
            (new_quote,  {'id': None, 'author_id': None, 'quote': None}),

            (einstein, {
                'id': None,
                'name': 'Albert Einstein',
                'birthday': datetime(month=3, day=14, year=1879),
                'bio': 'Won the 1921 Nobel Prize in Physics.'
            }),
            (change, {'id': None, 'name': 'change'}),
            (einstein_quote_I, {
                'id': None,
                'author_id': None,
                'quote': (
                    'The world as we have created it is a process of our thinking. '
                    'It cannot be changed without changing our thinking.'
                )
            })
        ]
    )
    def test_asdict(self, instance, expected):
        assert SQLAlchemyTableAdapter(instance).asdict() == expected

    ###################### _MixinColumnSQLAlchemyAdapter #######################
    @pytest.mark.parametrize(
        "instance, expected",
        [
            (new_author, {
                'id': None,
                'name': None,
                'birthday': None,
                'bio': None
            }),
            (new_tag,    {'id': None, 'name': None}),
            (new_quote,  {'id': None, 'author_id': None, 'quote': None}),

            (einstein, {
                'id': None,
                'name': 'Albert Einstein',
                'birthday': datetime(month=3, day=14, year=1879),
                'bio': 'Won the 1921 Nobel Prize in Physics.'
            }),
            (change, {'id': None, 'name': 'change'}),
            (einstein_quote_I, {
                'id': None,
                'author_id': None,
                'quote': (
                    'The world as we have created it is a process of our thinking. '
                    'It cannot be changed without changing our thinking.'
                )
            })
        ]
    )
    def test_under_fields_dict(self, instance, expected):
        assert SQLAlchemyTableAdapter(instance)._fields_dict == expected

    @pytest.mark.parametrize(
        "instance, field_name, expected",
        [
            (new_quote, 'id', None),
            (new_quote, 'author_id', None),
            (new_quote, 'quote', None),

            # (kennedy_quote_I, 'id', 3),
            # (kennedy_quote_I, 'author_id', 2),
            # (kennedy_quote_I, 'quote', 'If not us, who? If not now, when?'),
        ]
    )
    def test_dunder_getitem(self, instance, field_name, expected):
        assert SQLAlchemyTableAdapter(instance)[field_name] == expected

    @pytest.mark.parametrize(
        "instance, field_name, expected",
        [
            (new_quote, 'id', None),
            (new_quote, 'author_id', None),
            (new_quote, 'quote', None),

            # (kennedy_quote_I, 'id', 3),
            # (kennedy_quote_I, 'author_id', 2),
            # (kennedy_quote_I, 'quote', 'If not us, who? If not now, when?'),
        ]
    )
    def test_dunder_setitem(self, instance, field_name, expected):
        pass

    def test_dunder_delitem(self):
        pass

    def test_dunder_iter(self):
        pass

    def test_dunder_len(self):
        pass


class TestRelationship:

    # This tests MANYTOONE and MANYTOMANY
    # Directions can be ONETOONE, ONETOMANY, MANYTOONE & MANYTOMANY
    # At this time I don't think the other two provide any edge cases not
    # covered in this test suite.
    author_relationship, tags_relationship = inspect(Quote).relationships

    @pytest.mark.parametrize(
        "instance, relationship, expected",
        [
            (Quote(), author_relationship, {
                'name': 'author',
                'cls': Author,
                'columns': Author.__table__.columns,
                'direction': 'MANYTOONE',
                'single_relation': True,
                'related_tables': None
            }),
            (Quote(), tags_relationship, {
                'name': 'tags',
                'cls': Tag,
                'columns': Tag.__table__.columns,
                'direction': 'MANYTOMANY',
                'single_relation': False,
                'related_tables': InstrumentedList()
            }),

            (einstein_quote_I, author_relationship, {
                'name': 'author',
                'cls': Author,
                'columns': Author.__table__.columns,
                'direction': 'MANYTOONE',
                'single_relation': True,
                'related_tables': einstein
            }),
            (einstein_quote_I, tags_relationship, {
                'name': 'tags',
                'cls': Tag,
                'columns': Tag.__table__.columns,
                'direction': 'MANYTOMANY',
                'single_relation': False,
                'related_tables': InstrumentedList([change, deep_thoughts])
            }),
        ]
    )
    def test_dunder_init(self, instance, relationship, expected):
        assert Relationship(instance, relationship).__dict__ == expected

    @pytest.mark.parametrize(
        "instance, relationship, expected",
        [
            (Quote(), author_relationship, 0),
            (Quote(), tags_relationship, 0),

            (einstein_quote_I, author_relationship, 1),
            (einstein_quote_I, tags_relationship, 2),
        ]
    )
    def test_dunder_len(self, instance, relationship, expected):
        assert len(Relationship(instance, relationship)) == expected

    @pytest.mark.parametrize(
        "instance, relationship, expected",
        [
            (Quote(), author_relationship, []),
            (Quote(), tags_relationship, []),

            (einstein_quote_I, author_relationship, [einstein]),
            (einstein_quote_I, tags_relationship, [change, deep_thoughts]),
        ]
    )
    def test_dunder_iter(self, instance, relationship, expected):
        if expected == []:
            assert list(Relationship(instance, relationship)) == []
        else:
            for i, related_tables in enumerate(Relationship(instance, relationship)):
                assert related_tables == expected[i]


class TestScrapyDeclarativeMetaAdapter:

    def test_columns(self):
        pass

    def test_relationships(self):
        pass

    def test_filter_kwargs(self):
        pass

    def test_dunder_repr(self):
        pass

    def test_dunder_eq(self):
        pass


if __name__ == '__main__':

    print(SQLAlchemyTableAdapter(new_author).field_names())
