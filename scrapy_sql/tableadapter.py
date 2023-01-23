
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

from sqlalchemy.orm.collections import InstrumentedList


class QueryFilterIter:

    def __init__(self, query_filter):

        self.query_filter = query_filter

        if query_filter.single_relation:
            self._iterations = 1
            self.data = self.query_filter.data
        else:
            self._iterations = len(query_filter.data)
            # sets aren't subscritable
            self.data = list(self.query_filter.data)

        self._current_index = 0

    def __iter__(self):
        return self

    def __next__(self):
        while self._current_index < self._iterations:
            if self.query_filter.single_relation:
                member = self.data
            else:
                member = self.data[self._current_index]

            self._current_index += 1
            return member

        raise StopIteration


class QueryFilter:

    def __init__(self, relationship):
        """
        base_cls (type SQLAlchemy Table):
        relationship_attr (string): the name of the relationship attr in the base_cls
        relationship_info (dict):
        """

        self.cls = relationship.cls
        self.column_names = relationship.column_names
        self.single_relation = relationship.single_relation

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

    def __add__(self, other):
        """Given two QueryFilters add them all together"""

    def __str__(self):
        return str(self.data)

    __repr__ = __str__

    def __iter__(self):
        return QueryFilterIter(self)


class QueryFilterCollection:

    def __init__(self, parent_cls):
        """
        parent_table: the SQLAlchemy Table that hasa QueryFilterContainer
        """
        self.data = {
            r.name: QueryFilter(r)
            for r in parent_cls.relationships
        }

    def update(self, attr, value):
        if 'DOT' not in attr:
            raise BaseException()

        attribute_name, attribute_column = attr.split('DOT')
        self.data[attribute_name].update(attribute_column, value)

    def get(self, relationship_attr):
        return self.data[relationship_attr]

    @property
    def isempty(self):
        return all([_.isempty for _ in self.data.values()])

    def __str__(self):
        return str(
            {key: str(value) for key, value in self.data.items()}
        )

    __repr__ = __str__


class RelationshipIter:

    def __init__(self, relationship):
        self.relationship = relationship
        self._num_tables = len(relationship)
        self._current_index = 0

    def __iter__(self):
        return self

    def __next__(self):
        while self._current_index < self._num_tables:
            if self.relationship.single_relation:
                member = self.relationship.related_tables
            else:
                member = self.relationship.related_tables[self._current_index]

            self._current_index += 1
            return member

        raise StopIteration


class Relationship:
    def __init__(self, parent_cls, relationship):
        self.parent_cls = parent_cls

        self.name = relationship.class_attribute.key
        self.cls = relationship.mapper.class_
        self.column_names = self.cls().column_names

        self.direction = relationship.direction.name
        self.related_tables = getattr(self.parent_cls, self.name)

        self.single_relation = not isinstance(
            self.related_tables,
            InstrumentedList
        )

        # self.query_filter = self.parent_cls.query_filters.get(self.name)

    def replace_tables(self, tables):
        if (
            self.single_relation
            and isinstance(tables, self.cls)
        ):
            pass
        elif (
            self.single_relation
            and isinstance(tables, InstrumentedList)
        ):
            if len(tables) == 0:
                tables = None
            elif len(tables) == 1:
                tables = tables[0]
        elif (
            not self.single_relation
            and isinstance(tables, self.cls)
        ):
            tables = InstrumentedList([tables])

        if (
            not self.single_relation
            and isinstance(tables, (InstrumentedList, list, tuple))
            and all([isinstance(t, self.cls) for t in tables])
        ):
            pass

        setattr(self.parent_cls, self.name, tables)

    def extend(self, tables):
        if self.single_relation:
            raise BaseException()

        tables = InstrumentedList(arg_to_iter(tables))

        if all([isinstance(t, self.cls) for t in tables]):
            getattr(self.parent_cls, self.name).extend(tables)

    def __len__(self):
        if (
            self.single_relation
            and self.related_tables is None
        ):
            return 0
        elif (
            self.single_relation
        ):
            return 1
        return len(self.related_tables)

    def __iter__(self):
        return RelationshipIter(self)


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

        elif field_name in self.item.accepted_DOT_field_names:
            self.item.query_filters.update(field_name, value)

        elif field_name in self.item.relationship_names:

            # Gotta be a cleaner way to do this...
            relationship = [
                r for r in self.item.relationships if r.name == field_name
            ][0]

            if not relationship.single_relation:
                relationship.extend(value)
            else:
                relationship.replace_tables(value)

        else:
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
            # # SQLite doesn't support DATE/TIME types, but instead converts them to strings.
            # # Without this block of code this fact creates problems when
            # # querying a session obj.
            # # https://gist.github.com/danielthiel/8374607
            # if isinstance(column.type, (DATE, Date)):
            #     d[column.name] = func.DATE(column)
            # elif isinstance(column.type, (DATETIME, DateTime)):
            #     d[column.name] = func.DATETIME(column)
            # elif isinstance(column.type, (TIME, Time)):
            #     d[column.name] = func.TIME(column)
            # elif isinstance(column.type, TIMESTAMP):
            #     d[column.name] = func.TIMESTAMP(column)

            # Don't include autoincrement columns when filtering
            # to see which tables are already added to a session.
            # This is because tables in session will have a number already
            # while newly initalized tables will have `None` for the value.
            if (
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
    def query_filters(self):
        attr_dunder_name = '__query_filters'
        try:
            return getattr(self, attr_dunder_name)
        except AttributeError:
            setattr(self, attr_dunder_name, QueryFilterCollection(self))
        return getattr(self, attr_dunder_name)

    @property
    def relationships(self):
        return [Relationship(self, r) for r in inspect(self.__class__).relationships]

    @cached_property
    def relationship_names(self):
        return tuple(r.name for r in self.relationships)

    @property
    def accepted_DOT_field_names(self):
        result = []
        for r in self.relationships:
            for column_name in r.column_names:
                result.append(f'{r.name}DOT{column_name}')
        return tuple(result)

    def query_relationships(self, session):
        for name in self.relationship_names:
            query_filter = self.query_filters.get(name)

            if query_filter.isempty:
                continue

            if query_filter.single_relation:  # Can add None
                setattr(self, name, session.query(name, query_filter.data))
            else:
                tables = InstrumentedList()
                for query_dict in query_filter.data:
                    stored_table = session.query(name, query_dict)
                    # Don't have None in InstrumentedList
                    if (
                        stored_table is not None
                        and stored_table not in tables
                    ):
                        tables.append(stored_table)

                setattr(self, name, tables)

    # def filter_relationships(self, session):
    #     """
    #     Avoid adding duplicates to a session, by making sure each
    #     corresponding table is only held in the session once
    #     """
    #     for name in self.relationship_names:

    #         related_tables = getattr(self, name)

    #         if related_tables is None:
    #             continue
    #         if isinstance(related_tables, InstrumentedList):
    #             filtered_tables = InstrumentedList()

    #             for table in related_tables:
    #                 stored_table = session.return_stored_table(table)
    #                 if stored_table not in filtered_tables:
    #                     filtered_tables.append(stored_table)

    #             setattr(self, name, filtered_tables)

    #         else:
    #             setattr(self, name, session.return_stored_table(related_tables))

    def asdict(self):
        d = {}

        for column in self.columns:
            d[column.name] = getattr(self, column.name)

        for r in self.relationships:
            d[r.name] = r.related_tables

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

        for relationship in self.relationships:
            result += f"{relationship.name}={relationship.related_tables},"

        return result.strip(", ") + ")"

    def __str__(self):
        return self.__repr__()

    # @property
    # def __equality_columns(self):
    #     d = {}

    #     for column in self.columns:
    #         # Don't include autoincrement columns when filtering
    #         # to see which tables are already added to a session.
    #         # This is because tables in session will have a number already
    #         # while newly initalized tables will have `None` for the value.
    #         if (
    #             column.autoincrement is True
    #             or (
    #                 isinstance(column.type, Integer)
    #                 and column.primary_key is True
    #                 and column.foreign_keys == set()
    #             )
    #         ):
    #             continue

    #         # Foreign keys are typically sat with the relationship attrs
    #         # Since these may or may not be set for individual table instances
    #         # we'll avoid using them as a filter
    #         elif column.foreign_keys != set():
    #             continue

    #         else:
    #             d[column.name] = getattr(self, column.name)

    #     return d

    # def __eq__(self, other):
    #     if isinstance(other, self.__class__):
    #         return self.__equality_columns == other.__equality_columns

    #     elif isinstance(other, dict):
    #         for key, value in other.items():
    #             if not getattr(self, key) == value:
    #                 return False
    #         return True

    #     return False
