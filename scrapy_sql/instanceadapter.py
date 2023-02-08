# Project imports
from scrapy_sql.declarative_base import ScrapyDeclarativeBase

# Scrapy / Twisted Imports
from itemadapter.adapter import AdapterInterface  # Basically scrapy...

# SQLAlchemy Imports
from sqlalchemy import Table
from sqlalchemy.orm.decl_api import DeclarativeMeta

# 3rd 🎉 Imports
from collections.abc import KeysView
from typing import Any, Iterator, Optional, List


class _MixinColumnSQLAlchemyAdapter:

    @property
    def _fields_dict(self) -> dict:
        return self.asdict()

    def __getitem__(self, field_name: str) -> Any:
        return self.asdict()[field_name]

    def __setitem__(self, field_name: str, value: Any) -> None:
        if field_name in self._fields_dict:
            setattr(self.item, field_name, value)
        else:
            raise KeyError(
                f"{self.item_class_name} does not support field: {field_name}"
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


class SQLAlchemyInstanceAdapter(_MixinColumnSQLAlchemyAdapter, AdapterInterface):
    """
    https://github.com/scrapy/itemadapter#extending-itemadapter
    """

    accepted_classes = (DeclarativeMeta, ScrapyDeclarativeBase)

    def __init__(self, item) -> None:
        super().__init__(item)
        self.item_class = item.__class__
        self.item_class_name = self.item_class.__name__

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
        return item_class.column_names

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

    def asdict(self):
        return {
            column_name: getattr(self.item, column_name)
            for column_name in [c.name for c in self.item.columns]
        }
