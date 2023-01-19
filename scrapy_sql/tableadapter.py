
# For Item Adapters
import sqlalchemy
from sqlalchemy import Table, func, Integer
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.types import (
    DATE, Date,
    DATETIME, DateTime,
    TIME, Time,
    TIMESTAMP
)
from sqlalchemy import Column
from functools import cached_property

from itemadapter.adapter import AdapterInterface

from collections.abc import KeysView
from typing import Any, Iterator, Optional, List

from sqlalchemy.inspection import inspect

from scrapy.utils.misc import arg_to_iter

from pprint import pprint

import json
import ast

from scrapy_sql import utils

from sqlalchemy.orm.collections import InstrumentedList


class QueryFilter:

    def __init__(self, relationship_info):
        """
        base_cls (type SQLAlchemy Table):
        relationship_attr (string): the name of the relationship attr in the base_cls
        relationship_info (dict):
        """

        self.cls = relationship_info['cls']
        self.column_names = self.cls().column_names

        self.single_relation = relationship_info['single_relation']
        self.data = {} if self.single_relation else set()

    def update(self, column_name, value):

        if column_name not in self.column_names:
            raise TypeError()

        if self.single_relation:
            self.data.update({column_name: value})
        else:
            self.data.add({column_name: value})

    @property
    def isempty(self):
        return len(self.data) == 0

    def __str__(self):
        return str(self.data)

    __repr__ = __str__


class QueryFilterContainer:

    def __init__(self, parent_table):
        """
        parent_table: the SQLAlchemy Table that hasa QueryFilterContainer
        """
        self.relationship_attrs = parent_table.relationship_names
        self.relationship_info = parent_table.relationships

        self.data = {
            name: QueryFilter(self.relationship_info.get(name))
            for name in self.relationship_attrs
        }

    def update(self, attr, value):
        if 'DOT' not in attr:
            raise BaseException()

        attribute_name, attribute_column = attr.split('DOT')
        self.data[attribute_name].update(attribute_column, value)

    def get(self, relationship_attr):
        return self.data[relationship_attr]

    def __str__(self):
        return str(
            {key: str(value) for key, value in self.data.items()}
        )

    __repr__ = __str__


class _MixinColumnSQLAlchemyAdapter:

    _fields_dict: dict
    item: Any

    @property
    def _fields_dict(self):
        return self.asdict()

    def __getitem__(self, field_name: str) -> Any:
        return self.asdict()[field_name]

    def __setitem__(self, field_name: str, value: Any) -> None:
        # Column names can be sat directly
        if field_name in self.item.column_names:
            setattr(self.item, field_name, value)
            return

        if field_name in self.item.relationships:
            relationship = self.item.relationships.get(field_name)
            relationship_cls = relationship['cls']
            single_relation = relationship['single_relation']

            # Check for TypeErrors First
            if (
                single_relation
                and not isinstance(value, relationship_cls)
            ):
                error_msg = (
                    f"Cannot assign type {type(value)} to attr {self.item.__class__.__name__}.{field_name}.\n"
                    f"Can only assign type {relationship_cls}."
                )
                raise TypeError(error_msg)
            elif (
                not single_relation
                and isinstance(value, relationship_cls)
            ):
                pass  # This is fine, it'll just be appended to the current list
            elif (
                not single_relation
                # Needs to be InstrumentedList, list, etc...
                and not hasattr(value, '__iter__')
            ):
                error_msg = (
                    f"Cannot assign type {type(value)} to attr {self.item.__class__.__name__}.{field_name}.\n"
                    f"Can only assign iterables or type {relationship_cls}."
                )
                raise TypeError(error_msg)
            elif (
                not single_relation
                and not all([isinstance(v, relationship_cls) for v in value])
            ):
                error_msg = (
                    f"{self.item.__class__.__name__}.{field_name} can only be assigned to iterables containing "
                    f"only type {relationship_cls} or single values of type {relationship_cls}.\n"
                    f"{[type(x) for x in value]} cannot be assigned."
                )
                raise TypeError(error_msg)

            # Assign value to relationship attr
            if single_relation:
                setattr(self.item, field_name, value)
                return

            attr = getattr(self.item, field_name)
            if (
                not single_relation
                and isinstance(value, relationship_cls)
            ):
                attr.append(value)
            elif not single_relation:
                attr.extend(value)
            return

        if field_name in self.item.accepted_DOT_field_names:
            self.item.query_filters.update(field_name, value)

            if 'quotes.tables.Quote' in str(self.item.__class__):
                print('\n\n__setitem__')
                print(self.item)
                print(f"{self.item.query_filters=}")
                input('\n\n')

            return

        raise KeyError(
            f"{self.item.__class__.__name__} does not support field: {field_name}"
        )

    def __delitem__(self, field_name: str) -> None:
        """
        Can't or shouldn't delete a column from a SQLAlchemy Table.
        This needs to be defined for scrapy to work.
        """
        return None

    def __iter__(self) -> Iterator:
        return iter(self.asdict())

    def __len__(self) -> int:
        return len(self.asdict())


class SQLAlchemyTableAdapter(_MixinColumnSQLAlchemyAdapter, AdapterInterface):
    """
    https://github.com/scrapy/itemadapter#extending-itemadapter
    """

    accepted_classes = (
        Table,
        DeclarativeMeta
    )

    # When an item is scraped it's assigned to a TableAdapter class
    # Then obj keeps track of how many classes are adapted by this class.
    # Later this is used to query the sqlalchemy session for all the tables
    # currently in the session.
    seen_classes = set()

    @classmethod
    def is_item(cls, item: Any) -> bool:
        """
        This method is called when initalizing the ItemAdapter class.
        We also
        """

        """
        Return True if the adapter can handle the given item, False otherwise.
        The default implementation calls cls.is_item_class(item.__class__).

        Args:
            item (_type_): _description_

        Returns:
            boolean: _description_
        """
        if isinstance(
            item.__class__,
            cls.accepted_classes
        ):
            cls.seen_classes.add(item.__class__)
            return True
        return False

    def clear_seen_classes(cls):
        cls.seen_classes.clear()

    @classmethod
    def is_item_class(cls, item_class: type) -> bool:
        """
        Return True if the adapter can handle the given item class,
        False otherwise. Abstract (mandatory).

        Args:
            item_class (_type_): A python type that this class can handle

        Returns:
            boolean: _description_
        """
        return (
            isinstance(item_class, cls.accepted_classes)
            or item_class in cls.accepted_classes
        )

    @classmethod
    def get_field_names_from_class(cls, item_class: type) -> Optional[List[str]]:
        return item_class.asdict().keys()
        # return item_class.__table__.columns.keys()

    def field_names(self) -> KeysView:
        """
        Return a dynamic view of the table's column names. By default, this
        method returns the result of calling keys() on the current adapter,
        i.e., its return value depends on the implementation of the methods
        from the MutableMapping interface (more specifically, it depends on the
        return value of __iter__).

        You might want to override this method if you want a way to get all
        fields for an item, whether or not they are populated.
        For instance, Scrapy uses this method to define column names when
        exporting items to CSV.

        Returns:
            _type_: _description_
        """
        return KeysView(self.item.asdict().keys())

    def asdict(self) -> dict:
        return self.item.asdict()


class ScrapyDeclarativeMetaAdapter:
    """
    Scrapy default logging of an item writes the item to the log file, in a
    format identical to str(my_dict).

    sqlalchemy.orm.decl_api.DeclarativeMeta classes use
    sqlalchemy.ext.declarative.declarative_base() classes as parent classes.

    If sqlalchemy.orm.decl_api.DeclarativeMeta classes instead have dual
    inheritence by adding this class Scrapy logging output can behave
    normally by utilizing this classes' __repr__ method
    """

    @property
    def query_filters(self):
        attr_dunder_name = '__query_filters'
        try:
            return getattr(self, attr_dunder_name)
        except AttributeError:
            setattr(self, attr_dunder_name, QueryFilterContainer(self))
        return getattr(self, attr_dunder_name)

    @cached_property
    def columns(self):
        return self.__table__.columns

    @cached_property
    def column_names(self):
        return tuple(column.name for column in self.columns)

    @property
    def query_filter(self):
        """
        Mainly used by session.query(class).filter_by().first() calls
        """
        d = {}

        for column in self.columns:
            # SQLite doesn't support DATE/TIME types, but instead converts them to strings.
            # Without this block of code this fact creates problems when
            # querying a session obj.
            # https://gist.github.com/danielthiel/8374607
            if isinstance(column.type, (DATE, Date)):
                d[column.name] = func.DATE(column)
            elif isinstance(column.type, (DATETIME, DateTime)):
                d[column.name] = func.DATETIME(column)
            elif isinstance(column.type, (TIME, Time)):
                d[column.name] = func.TIME(column)
            elif isinstance(column.type, TIMESTAMP):
                d[column.name] = func.TIMESTAMP(column)

            # Don't include autoincrement columns when filtering
            # to see which tables are already added to a session.
            # This is because tables in session will have a number already
            # while newly initalized tables will have `None` for the value.
            elif (
                column.autoincrement is True
                or (
                    isinstance(column.type, Integer)
                    and column.primary_key is True
                    and column.foreign_keys == set()
                )
            ):
                continue

            # Foreign keys are typically sat with the relationship attrs
            # Since these may or may not be set for individual table instances
            # we'll avoid using them as a filter
            elif column.foreign_keys != set():
                continue

            else:
                d[column.name] = getattr(self, column.name)

        return d

    @property
    def relationships(self):
        d = {}
        for r in inspect(self.__class__).relationships:
            name = r.class_attribute.key
            cls = r.mapper.class_
            direction = r.direction.name
            related_tables = getattr(self, name)
            single_relation = not isinstance(related_tables, InstrumentedList)

            d[name] = {
                'cls': cls,
                'direction': direction,
                'related_tables': related_tables,
                'single_relation': single_relation,
            }
        return d

    @cached_property
    def relationship_names(self):
        return tuple(name for name in self.relationships.keys())

    def query_relationships(self, session):
        for name in self.relationship_names:
            query_filter = self.query_filters.get(name)

            # input(f"{query_filter=}")
            if query_filter.isempty:
                continue

            if query_filter.single_relation:  # Can add None
                attr = session.query(
                    query_filter.cls
                ).filter_by(
                    **query_filter.data
                ).first()
            else:
                attr = set()
                for query_dict in query_filter.data:
                    exists = session.query(
                        query_filter.cls
                    ).filter_by(
                        **query_dict
                    ).first()

                    if exists is not None:  # Don't have None in InstrumentedList
                        attr.add(exists)

                attr = InstrumentedList(attr)
            setattr(self, name, attr)

    def filter_relationships(self, session):
        """
        Avoid adding duplicates to a session, by making sure each
        corresponding table is only held in the session once
        """
        for name, info in self.relationships.items():
            single_relation = info['single_relation']
            related_tables = info['related_tables']

            if (
                single_relation
                and related_tables is not None
            ):
                setattr(
                    self,
                    name,
                    utils.filter_table(session, related_tables)
                )
            elif not single_relation:
                filtered_tables = set()
                for table in related_tables:
                    filtered_tables.add(
                        utils.filter_table(session, table)
                    )
                setattr(
                    self,
                    name,
                    InstrumentedList(filtered_tables)
                )

    def combine_relationships(self, other):
        """
        When filtering out table objs, it's possible that two equal objs
        (equal based on column fields) have unequal relationships.
        """
        pass

    @property
    def accepted_DOT_field_names(self):
        """
        Suppose we're scraping https://quotes.toscrape.com/
        If we have a `Quote` and `Author` table we could define the
        relationship in the `Quote`.

        To determine which `Author` table belongs to that relationship we'd
        need to query an active session like

        session.query(
            Author
        ).filter_by(
            {'name': 'authors_name'}
        ).first()

        to make that query we allow an `authorDOTname` field to be scraped
        for the quote table.
        """
        result = []
        for r in inspect(self.__class__).relationships:
            for column_name in r.mapper.class_().column_names:
                result.append(f'{r.class_attribute.key}DOT{column_name}')
        return tuple(result)

    def asdict(self):
        d = {}

        for column in self.columns:
            d[column.name] = getattr(self, column.name)

        for name, info in self.relationships.items():
            d[name] = info['related_tables']

        for name in self.accepted_DOT_field_names:
            d[name] = None

        return d

    def __repr__(self):
        result = f"{self.__class__.__name__}("

        for column in self.columns:
            column_value = getattr(self, column.name)
            if isinstance(column_value, str):
                result += f"{column.name}='{column_value}', "
            else:  # No quotation marks if not a string
                result += f"{column.name}={column_value}, "

        for name, info in self.relationships.items():
            result += f"{name}={info['related_tables']}, "

        return result.strip(", ") + ")"

    def __str__(self):
        return self.__repr__()
