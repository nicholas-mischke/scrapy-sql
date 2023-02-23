
# Project Imports
from .utils import column_value_is_subquery

from sqlalchemy.orm.base import instance_dict, instance_state, instance_str

# Scrapy / Twisted Imports
from descriptors import classproperty
from typing import Any, Iterator, Optional, List
from collections.abc import KeysView
from itemadapter.adapter import AdapterInterface  # Basically scrapy...

# SQLAlchemy Imports
from sqlalchemy import select, Integer, Numeric
from sqlalchemy.orm.decl_api import DeclarativeAttributeIntercept
from sqlalchemy.orm.attributes import set_attribute, get_attribute, del_attribute
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import func

from sqlalchemy.orm.attributes import ScalarObjectAttributeImpl
from sqlalchemy.types import DATE, Date, DATETIME, DateTime, TIME, Time, TIMESTAMP

date_types = (DATE, Date, DATETIME, DateTime, TIME, Time, TIMESTAMP)

# 3rd 🎉 Imports


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
        columns = item_class.__table__.columns
        column_names = [c.name for c in columns]

        relationships = inspect(item_class).relationships
        relationship_names = [
            relationship.class_attribute.key
            for relationship in relationships
        ]

        return column_names + relationship_names

    def __init__(self, item) -> None:
        super().__init__(item)
        self.item_class = item.__class__
        self.item_class_name = self.item_class.__name__

    def asdict(self):
        return {
            attr: getattr(self.item) for attr in
            SQLAlchemyInstanceAdapter.get_field_names_from_class(
                self.item_class)
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
    def relationships(cls):
        return inspect(cls).relationships

    @property
    def unloaded_columns(self):
        result = []
        for column in self.columns:
            if getattr(self, column.name) is None:
                result.append(column)
        return result

    @property
    def loaded_columns(self):
        result = []
        for column in self.columns:
            value = getattr(self, column.name)
            if value is not None:
                result.append(column)

        return result

    @property
    def params(self):
        return {
            column.name: getattr(self, column.name)
            for column in self.loaded_columns
        }

    def subquery(self, *return_columns):

        where_args = []

        for column in self.loaded_columns:
            value = getattr(self, column.name)

            # TODO fix this to work with SQLite
            if isinstance(column.type, date_types):
                continue
                # func.DATE(column)

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
            # is scalar
            and isinstance(return_columns[0].type, (Integer, Numeric))
        ):
            return subquery.scalar_subquery()
        return subquery

    def __repr__(self):

        # Does not currently work well for nested relationships
        attrs = SQLAlchemyInstanceAdapter.get_field_names_from_class(
            self.__class__
        )

        result = f"{self.__class__.__name__}("

        for attr in attrs:
            value = getattr(self, attr)
            if isinstance(value, str):
                result += f"{attr}='{value}', "
            else:  # No quotation marks if not a string
                result += f"{attr}={value}, "

        return result.strip(", ") + ")"

    __str__ = __repr__

    @classmethod
    def from_repr(cls, repr):
        pass
