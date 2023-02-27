
# Project Imports
from .utils import classproperty, column_value_is_subquery, is_scalar_column

# Scrapy / Twisted Imports
from itemadapter.adapter import AdapterInterface  # Basically scrapy...

# SQLAlchemy Imports
from sqlalchemy import func, select
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.attributes import (del_attribute, get_attribute,
                                       set_attribute)
from sqlalchemy.orm.base import object_mapper
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.orm.decl_api import DeclarativeAttributeIntercept
from sqlalchemy.sql.sqltypes import (JSON, Boolean, Date, DateTime, Integer,
                                     Numeric, String, Time)

# 3rd 🎉 Imports
from collections.abc import KeysView
from datetime import datetime
import json
from typing import Any, Iterator, List, Optional


class _MixinColumnSQLAlchemyAdapter:

    @property
    def _fields_dict(self) -> dict:
        return self.asdict()

    def __setitem__(self, field_name: str, value: Any) -> None:
        try:
            set_attribute(self.item, field_name, value)
        except KeyError:
            raise KeyError(
                f"{self.item_class_name} does NOT support field: {field_name}\n"
                f"Supported fields are {', '.join(list(self.field_names))}."
            )

    def __getitem__(self, field_name: str) -> Any:
        get_attribute(self.item, field_name)

    def __delitem__(self, field_name: str) -> None:
        del_attribute(self.item, field_name)

    def __iter__(self) -> Iterator:
        return iter(self.asdict())

    def __len__(self) -> int:
        return len(self.asdict())


class SQLAlchemyInstanceAdapter(_MixinColumnSQLAlchemyAdapter, AdapterInterface):
    """
    https://github.com/scrapy/itemadapter#extending-itemadapter
    """

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


class ScrapyDeclarativeBase:

    @classproperty
    def columns(cls):
        return cls.__table__.columns

    @classproperty
    def column_names(cls):
        return tuple(c.name for c in cls.columns)

    @classproperty
    def column_name_to_column_obj_map(cls):
        return {
            column.name: column
            for column in cls.columns
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
        returns a dictionary to be used with session.execute(statment, params)
        """
        return {
            column.name: getattr(self, column.name)
            for column in self.loaded_columns
        }

    @classmethod
    def subquery_from_dict(cls, instance_kwargs, *return_columns):
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
            return_columns = tuple(mapper._pks_by_table[self.__table__])
        else:
            return_columns = tuple(return_columns)

        where_args = []

        for column in self.loaded_columns:
            value = getattr(self, column.name)

            # TODO fix this to work with SQLite
            if isinstance(column.type, (Date, DateTime, Time)):
                continue
                # sqlalchemy.func.DATE(column)

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
        return subquery

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

    def __repr__(self):

        result = f"{self.__class__.__name__}("

        for name in self.column_names:
            value = getattr(self, name)
            if isinstance(value, str):
                result += f'{name}="{value}", '
            else:  # No quotation marks if not a string
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
                    result += f"{repr(instance)}, "
                result = result.strip(', ') + ']'
            else:
                result += f"{relationship_name}={repr(related_instances)}, "

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
            raise TypeError

        instance = cls()
        init_kwargs = {}

        string = string.lstrip(f'{cls.__name__}(')
        string = string.rstrip(')')

        kwargs_location = {
            name: None
            for name in cls.column_names + cls.relationship_names
        }

        for name in cls.column_names:
            kwargs_location[name] = string.index(f'{name}=')

        for relationship in cls.relationships:
            name = relationship.class_attribute.key
            relationship_cls = relationship.entity.class_manager.class_

            related_instances = getattr(instance, name)
            if isinstance(related_instances, InstrumentedList):
                try:
                    substring = f'{name}=[{relationship_cls.__name__}('
                    kwargs_location[name] = string.index(substring)
                except ValueError:
                    substring = f'{name}=[]'
                    kwargs_location[name] = string.index(substring)
            else:
                substring = f'{name}={relationship_cls.__name__}('
                kwargs_location[name] = string.index(substring)

        indicies = sorted(kwargs_location.values()) + [len(string)]
        kwargs_location = {
            name: (value, min([i for i in indicies if i > value]))
            for name, value in kwargs_location.items()
        }

        for name in cls.column_names:
            start, stop = kwargs_location[name]
            attr_string = string[start:stop]
            attr_value = attr_string.lstrip(f"{name}=").rstrip(", ").strip('"')

            column_type = cls.column_name_to_column_obj_map[name].type

            if attr_value == 'None':
                attr_value = None
            elif isinstance(column_type, Boolean):
                attr_value = True if attr_value == 'True' else False
            elif isinstance(column_type, String):
                pass  # already a string
            elif isinstance(column_type, Integer):
                attr_value = int(attr_value)
            elif isinstance(column_type, Numeric):
                attr_value = float(attr_value)
            elif isinstance(column_type, Date):
                attr_value = datetime.strptime(attr_value, date_format).date()
            elif isinstance(column_type, Time):
                attr_value = datetime.strptime(attr_value, time_format).time()
            elif isinstance(column_type, DateTime):
                attr_value = datetime.strptime(attr_value, datetime_format)
            elif isinstance(column_type, JSON):
                attr_value = json.loads(attr_value)

            init_kwargs[name] = attr_value

        for relationship in cls.relationships:
            name = relationship.class_attribute.key
            relationship_cls = relationship.entity.class_manager.class_

            start, stop = kwargs_location[name]
            attr_string = string[start:stop]
            attr_value = attr_string.lstrip(f"{name}=").rstrip(", ").strip('"')

            related_instances = getattr(instance, name)
            if isinstance(related_instances, InstrumentedList):
                if attr_value == '[]':
                    attr_value = InstrumentedList()
                else:
                    attr_values = attr_value.lstrip('[').rstrip(']').split(
                        f", {relationship_cls.__name__}("
                    )

                    for i, value in enumerate(attr_values):
                        if not value.startswith(f"{relationship_cls.__name__}("):
                            attr_values[i] = f"{relationship_cls.__name__}({value}"

                    attr_value = InstrumentedList()
                    for value in attr_values:
                        attr_value.append(
                            relationship_cls.from_repr(value)
                        )
            else:
                if attr_value == 'None':
                    attr_value = None
                else:
                    attr_value = relationship_cls.from_repr(attr_value)

            init_kwargs[name] = attr_value

        return cls(**init_kwargs)
