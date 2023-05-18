
# Project Imports
from .utils import (
    classproperty, is_scalar_column,
    column_value_is_subquery, subquery_to_string
)

# Scrapy / Twisted Imports
from itemadapter.adapter import AdapterInterface  # Basically scrapy...

# SQLAlchemy Imports
import sqlalchemy
from sqlalchemy import func, select
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm.attributes import (
    del_attribute,
    get_attribute,
    set_attribute
)
from sqlalchemy.orm.base import object_mapper
from sqlalchemy.orm.base import ONETOMANY, MANYTOONE, MANYTOMANY  # ONETOONE not listed
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.orm.decl_api import DeclarativeAttributeIntercept
from sqlalchemy.sql.sqltypes import (
    JSON, Boolean, Date, DateTime, Integer,
    Numeric, String, Time
)

# 3rd 🎉 Imports
from collections.abc import KeysView
from datetime import datetime
import json
from typing import Any, Iterator, List, Optional
# here for testing
from pprint import pprint


class _MixinColumnSQLAlchemyAdapter:

    @property
    def _fields_dict(self) -> dict:
        return self.asdict()

    def __delitem__(self, field_name: str) -> None:
        """
        This resets the attr to None or an empty InstrumentedList
        """
        del_attribute(self.item, field_name)

    def __getitem__(self, field_name: str) -> Any:
        return get_attribute(self.item, field_name)

    def __setitem__(self, field_name: str, value: Any) -> None:
        try:
            set_attribute(self.item, field_name, value)
        except KeyError:
            raise KeyError(
                f"{self.item_class_name} does NOT support field: {field_name}\n"
                f"Supported fields are {', '.join(list(self._fields_dict))}."
            )

    def __iter__(self) -> Iterator:
        return iter(self.asdict())

    def __len__(self) -> int:
        return len(self.asdict())


class SQLAlchemyInstanceAdapter(_MixinColumnSQLAlchemyAdapter, AdapterInterface):
    """
    https://github.com/scrapy/itemadapter#extending-itemadapter
    """

    # Does not currently accept Core tables obj, just ORM classes/instances
    accepted_classes = (DeclarativeAttributeIntercept, )

    @classmethod
    def is_item(cls, item: Any) -> bool:
        """
        This method is called when initalizing the ItemAdapter class.

        Return True if the adapter can handle the given item, False otherwise.
        The default implementation calls cls.is_item_class(item.__class__).
        """
        return isinstance(type(item), cls.accepted_classes)

    @classmethod
    def is_item_class(cls, item_class: type) -> bool:
        """
        Return True if the adapter can handle the given item class,
        False otherwise. Abstract (mandatory).
        """
        return type(item_class) in cls.accepted_classes

    @classmethod
    def get_field_names_from_class(cls, item_class: type) -> Optional[List[str]]:
        return list(item_class.column_names) \
            + list(item_class.relationship_names)

    def __init__(self, item) -> None:
        super().__init__(item)
        self.item_class = item.__class__
        self.item_class_name = self.item_class.__name__

    def asdict(self):
        attrs = self.item.column_names + self.item.relationship_names
        return {
            attr: getattr(self.item, attr) for attr in attrs
        }

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
        """
        return KeysView(self.asdict().keys())


class ScrapyDeclarativeBase: #(DeclarativeBase):
    # __abstract__ = True # Make sure SQLAlchemy doesn't try and create a table from this cls

    @classproperty
    def sorted_tables(cls):
        return cls.metadata.sorted_tables

    @classproperty
    def sorted_entities(cls):
        mappers = cls._sa_registry.mappers

        entities = [mapper.entity for mapper in mappers]
        table_entity_map = {entity.__table__: entity for entity in entities}

        result = []
        for table in cls.sorted_tables:
            try:
                result.append(table_entity_map[table])
            except KeyError:  # table without entity, most likely a join table
                pass
        return result

    @classproperty
    def tablename_to_entity_map(cls):
        """Used in from_repr classmethod, specifically with subqueries"""
        entites = [mapper.entity for mapper in cls._sa_registry.mappers]
        d = {entity.__table__.name: entity for entity in entites}

        for table in cls.sorted_tables:  # Join tables, that don't have an entity
            if not isinstance(table, ScrapyDeclarativeBase):
                d.setdefault(table.name, table)

        return d

    @classproperty
    def columns(cls):
        return cls.__table__.columns

    @classproperty
    def column_names(cls):
        return tuple(c.name for c in cls.columns)

    @classproperty
    def column_name_to_column_obj_map(cls):
        """
        Used in `subquery` method to allow for column names to be passed
        instead of column objs
        """
        return {
            column.name: column
            for column in cls.columns
        }

    @classproperty
    def relationships(cls):
        return inspect(cls).relationships

    @classproperty
    def relationship_names(cls):
        return tuple(r.class_attribute.key for r in cls.relationships)

    @classproperty
    def relationship_name_to_relationship_obj_map(cls):
        return {
            relationship.class_attribute.key: relationship
            for relationship in cls.relationships
        }

    @property
    def unloaded_columns(self):
        return tuple(
            column for column in self.columns
            if getattr(self, column.name) is None
        )

    @property
    def loaded_columns(self):
        return tuple(
            column for column in self.columns
            if getattr(self, column.name) is not None
        )

    @property
    def params(self):
        """
        returns a dictionary to be used with session.execute(stmt, params)
        """
        return {
            column.name: getattr(self, column.name)
            for column in self.loaded_columns
        }

    @classmethod
    def subquery_from_dict(cls, *return_columns, **instance_kwargs):
        instance = cls(**instance_kwargs)
        return instance.subquery(*return_columns)

    def subquery(self, *return_columns):

        return_columns = list(return_columns)

        # Allow strings to be passed in as arguments for columns
        for i, column in enumerate(return_columns):
            if isinstance(column, str):
                return_columns[i] = self.column_name_to_column_obj_map[column]

        if return_columns == []:  # Default to pks
            mapper = object_mapper(self)
            return_columns = mapper._pks_by_table[self.__table__]

        return_columns = tuple(return_columns)

        where_args = []

        for column in self.loaded_columns:
            value = getattr(self, column.name)

            # TODO SQLite doesn't work with DATE types, fix this so they will
            if isinstance(column.type, Date):
                continue
                # sqlalchemy.func.DATE(column)
            elif isinstance(column.type, DateTime):
                continue
                # sqlalchemy.func.DATETIME(column)
            elif isinstance(column.type, Time):
                continue
                # sqlalchemy.func.TIME(column)

            # Subqueries don't work for whatever reason.
            # This could probably be worked around if need be
            if column_value_is_subquery(value):
                continue

            where_args.append(column)

        subquery = select(*return_columns).where(
            *tuple(
                column == getattr(self, column.name)
                for column in where_args
            )
        )

        if (
            len(return_columns) == 1  # Single return column
            and is_scalar_column(return_columns[0])
        ):
            return subquery.scalar_subquery()
        return subquery.subquery()

    def __repr__(self):

        result = f"{self.__class__.__name__}("

        for name in self.column_names:
            value = getattr(self, name)

            if column_value_is_subquery(value):
                value = subquery_to_string(value)
            elif isinstance(value, str):  # quotation marks if string
                value = f'"{value}"'

            result += f"{name}={value}, "

        for relationship in self.relationships:
            relationship_name = relationship.class_attribute.key
            relationship_cls = relationship.entity.class_manager.class_

            related_instances = getattr(self, relationship_name)
            if related_instances is None:
                result += f"{relationship_name}={None}, "
            elif isinstance(related_instances, InstrumentedList):
                result += f'{relationship_name}=['
                for instance in related_instances:
                    result += f"{relationship_cls.__repr__(instance)}, "
                result = result.strip(', ') + ']'
            else:
                result += f"{relationship_name}={relationship_cls.__repr__(related_instances)}, "

        return result.strip(", ") + ")"

    __str__ = __repr__

    @classmethod
    def from_repr(
        cls,
        string,
        date_format='%Y-%m-%d',
        time_format='%H:%M:%S',
        datetime_format='%Y-%m-%d %H:%M:%S'
    ):
        if not string.startswith(cls.__name__):
            raise TypeError()

        kwargs = cls._from_repr_kwargs(string)

        for column in cls.columns:
            attr = column.name
            kwargs[attr] = cls._from_repr_columns(
                column,
                kwargs[attr],
                date_format,
                time_format,
                datetime_format
            )

        for relationship in cls.relationships:
            attr = relationship.class_attribute.key
            kwargs[attr] = cls._from_repr_relationships(
                relationship,
                kwargs[attr]
            )

        return cls(**kwargs)

    @classmethod
    def _from_repr_kwargs(cls, string):
        instance = cls()
        attrs = cls.column_names + cls.relationship_names

        string = string.lstrip(f'{cls.__name__}(')
        string = string.rstrip(')')

        kwargs = {attr: None for attr in attrs}

        for attr in cls.column_names:
            kwargs[attr] = string.index(f'{attr}=')

        for relationship in cls.relationships:
            attr = relationship.class_attribute.key
            r_cls_name = relationship.entity.class_manager.class_.__name__

            related_instances = getattr(instance, attr)
            if isinstance(related_instances, InstrumentedList):
                try:
                    index = string.index(f'{attr}=[{r_cls_name}(')
                except ValueError:
                    index = string.index(f'{attr}=[]')
            else:
                try:
                    index = string.index(f'{attr}={r_cls_name}(')
                except ValueError:
                    index = string.index(f'{attr}=None')
            kwargs[attr] = index

        indicies = sorted(kwargs.values()) + [len(string)]
        for attr, start in kwargs.items():
            stop = min([i for i in indicies if i > start])

            value = string[start:stop]
            value = value.lstrip(f'{attr}=').rstrip(", ").strip('"')

            kwargs[attr] = value

        return kwargs

    @classmethod
    def _from_repr_columns(
        cls,
        column,
        string,
        date_format='%Y-%m-%d',
        time_format='%H:%M:%S',
        datetime_format='%Y-%m-%d %H:%M:%S'
    ):

        # First try to convert a subquery
        try:
            return cls._from_repr_subquery(column, string)
        except BaseException: # TODO make this a meaningful Exception
            pass

        if string == 'None':
            return None
        elif isinstance(column.type, Boolean):
            return True if string == 'True' else False

        elif isinstance(column.type, String):
            return string

        elif isinstance(column.type, Integer):
            return int(string)
        elif isinstance(column.type, Numeric):
            return float(string)

        elif isinstance(column.type, Date):
            return datetime.strptime(string, date_format).date()
        elif isinstance(column.type, Time):
            return datetime.strptime(string, time_format).time()
        elif isinstance(column.type, DateTime):
            return datetime.strptime(string, datetime_format)

        elif isinstance(column.type, JSON):
            return json.loads(string)

        raise BaseException()  # TODO make this a meaningful Exception

    @classmethod
    def _from_repr_subquery(cls, column, string):
        """Run SELECT * FROM table back into orm subqueries"""

        if string.startswith('SELECT'):
            return sqlalchemy.text(string)
        raise BaseException()  # TODO make this a meaningful Exception

    @classmethod
    def _from_repr_relationships(cls, relationship, string):
        attr = relationship.class_attribute.key
        r_cls = relationship.entity.class_manager.class_

        instance = cls()
        related_instances = getattr(instance, attr)

        if not isinstance(related_instances, InstrumentedList):
            if string == 'None':
                return None
            return r_cls.from_repr(string)

        if string == '[]':
            return InstrumentedList()

        # TODO this split would be better with regex
        string = string.lstrip('(').rstrip(')')
        strings = string \
            .lstrip('[').rstrip(']') \
            .split(f", {r_cls.__name__}(")

        # TODO Splitting with regex would remove the need for this code block
        for i, string in enumerate(strings):
            if i == 0:
                continue
            strings[i] = f"{r_cls.__name__}({string}"

        return InstrumentedList([
            r_cls.from_repr(string)
            for string in strings
        ])
