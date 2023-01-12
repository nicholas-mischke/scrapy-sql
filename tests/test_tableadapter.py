
import pytest

from datetime import datetime

from sqlalchemy import Table
from sqlalchemy.orm.decl_api import DeclarativeMeta

import scrapy

from scrapy_sql import SQLAlchemyTableAdapter



class TestScrapyDeclarativeMetaAdapter:

    def test_column_names(self, einstein):
        assert einstein.column_names == (
            'id',
            'name',
            'birthday',
            'bio'
        )

    def test_asdict(self, einstein):
        assert einstein.asdict() == {
            'id': 1,
            'name': 'Albert Einstein',
            'birthday': datetime(year=1879, month=3, day=14),
            'bio': 'Won the 1921 Nobel Prize in Physics for his paper explaining the photoelectric effect.'
        }

    def test_dunder_repr(self, einstein):
        assert einstein.__repr__() == (
            "Author(id=1, "
            "name='Albert Einstein', "
            "birthday=1879-03-14 00:00:00, "
            "bio='Won the 1921 Nobel Prize in Physics for his paper explaining "
            "the photoelectric effect.')"
        )

    def test_dunder_str(self, einstein):
        assert einstein.__str__() == (
            "Author(id=1, "
            "name='Albert Einstein', "
            "birthday=1879-03-14 00:00:00, "
            "bio='Won the 1921 Nobel Prize in Physics for his paper explaining "
            "the photoelectric effect.')"
        )


class TestSQLAlchemyTableAdapter:

    # class methods
    @pytest.mark.parametrize(
        "item_class, expected",
        [
            (Table, True),
            (DeclarativeMeta, True),
            (dict, False),
            (scrapy.Item, False)
        ]
    )
    def test_is_item_class(self, item_class, expected):
        """Return True if the adapter can handle the given item class, False otherwise."""
        assert SQLAlchemyTableAdapter.is_item_class(item_class) == expected

    def test_is_item_class_II(self, db_tables):
        for table in db_tables:
            assert SQLAlchemyTableAdapter.is_item_class(table) == True

    def test_is_item(self, einstein):
        assert SQLAlchemyTableAdapter.is_item(einstein) == True

    def test_get_field_names_from_class(self, author_table):
        assert SQLAlchemyTableAdapter.get_field_names_from_class(
            author_table
        ) == [
            'id',
            'name',
            'birthday',
            'bio'
        ]

    def test_field_names(self, einstein_tableadapter):
        einstein_tableadapter.field_names == [
            'id',
            'name',
            'birthday',
            'bio'
        ]

    def test_asdict(self, einstein_tableadapter):
        assert einstein_tableadapter.asdict() == {
            'id': 1,
            'name': 'Albert Einstein',
            'birthday': datetime(year=1879, month=3, day=14),
            'bio': 'Won the 1921 Nobel Prize in Physics for his paper explaining the photoelectric effect.'
        }

    # _MixinColumnSQLAlchemyAdapter
    def test_dunder_getitem(self, einstein_tableadapter):
        for attr, value in [
            ('id', 1),
            ('name', 'Albert Einstein'),
            ('birthday', datetime(year=1879, month=3, day=14)),
            ('bio', 'Won the 1921 Nobel Prize in Physics for his paper explaining the photoelectric effect.')
        ]:
            assert einstein_tableadapter[attr] == value

    def test_dunder_setitem(self, einstein_tableadapter):
        assert einstein_tableadapter['id'] == 1
        einstein_tableadapter['id'] = 2
        assert einstein_tableadapter['id'] == 2

        # Make einstein a function scope fixture
        einstein_tableadapter['id'] = 1

    def test_dunder_delitem(self, einstein_tableadapter):
        """
        del einstein_tableadapter['column']
        should do nothing
        """
        del einstein_tableadapter['id']
        assert 'id' in einstein_tableadapter.asdict() # does not delete

    def test_dunder_iter(self, einstein_tableadapter):
        item_asdict = einstein_tableadapter.item.asdict()
        for key, value in einstein_tableadapter.items():
            assert item_asdict[key] == value

    def test_dunder_len(self, einstein_tableadapter):
        assert len(einstein_tableadapter) == 4
