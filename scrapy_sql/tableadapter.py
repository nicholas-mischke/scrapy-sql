
# For Item Adapters
from sqlalchemy import Table
from sqlalchemy.orm.decl_api import DeclarativeMeta

from itemadapter.adapter import AdapterInterface

from collections.abc import KeysView
from typing import Any, Iterator, Optional, List


class _MixinColumnSQLAlchemyAdapter:

    _fields_dict: dict
    item: Any

    @property
    def _fields_dict(self):
        return self.asdict()

    def __getitem__(self, field_name: str) -> Any:
        return self.asdict()[field_name]

    def __setitem__(self, field_name: str, value: Any) -> None:
        if field_name in self._fields_dict:
            setattr(self.item, field_name, value)
        else:
            raise KeyError(
                f"{self.item.__class__.__name__} does not support field: {field_name}")

    def __delitem__(self, field_name: str) -> None:
        """
        Can't or shouldn't delete a column from a SQLAlchemy Table.
        This needs to be defined for scrapy to work.
        """
        return None
        # del self.item[field_name]

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

    @classmethod
    def is_item(cls, item: Any) -> bool:
        """
        Return True if the adapter can handle the given item, False otherwise.
        The default implementation calls cls.is_item_class(item.__class__).

        Args:
            item (_type_): _description_

        Returns:
            boolean: _description_
        """
        return isinstance(item.__class__, SQLAlchemyTableAdapter.accepted_classes)

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
        return item_class in SQLAlchemyTableAdapter.accepted_classes

        # for item_class in SQLAlchemyTableAdapter.accepted_classes:
        #     if item_class == item_class:
        #         return True
        # return False

    @classmethod
    def get_field_names_from_class(cls, item_class: type) -> Optional[List[str]]:
        return item_class.__table__.columns.keys()

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
        return KeysView(self.item.__table__.columns.keys())

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
    def columns(self):
        return self.__table__.columns

    def asdict(self):
        keys = self.columns.keys()
        values = [getattr(self, key) for key in keys]
        return dict(zip(keys, values))

    def __repr__(self):
        result = f"{self.__class__.__name__}("
        for column, column_value in self.asdict().items():
            if isinstance(column_value, str):
                result += f"{column}='{column_value}', "
            else:  # No quotation marks if not a string
                result += f"{column}={column_value}, "
        return result.strip(", ") + ")"

    def __str__(self):
        return self.__repr__()
