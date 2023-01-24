
# For Item Adapters
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


class _MixinColumnSQLAlchemyAdapter:

    @property
    def _fields_dict(self) -> dict:
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

    accepted_classes = (DeclarativeMeta, Table)

    def __init__(self, item) -> None:
        super().__init__(item)
        self.item_class = item.__class__

    # Begin class methods
    @classmethod
    def is_item(cls, item: Any) -> bool:
        """
        This method is called when initalizing the ItemAdapter class.

        Return True if the adapter can handle the given item, False otherwise.
        The default implementation calls cls.is_item_class(item.__class__).
        """
        return isinstance(item.__class__, cls.accepted_classes)

    @classmethod
    def is_item_class(cls, item_class: type) -> bool:
        """
        Return True if the adapter can handle the given item class,
        False otherwise. Abstract (mandatory).
        """
        return isinstance(item_class(), cls.accepted_classes)

    @classmethod
    def get_field_names_from_class(cls, item_class: type) -> Optional[List[str]]:
        return item_class.asdict().keys()

    @classmethod
    def get_columns(cls, item_class):
        return item_class.__table__.columns

    # End class methods

    # Begin instance methods
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
        return KeysView(self.item.asdict().keys())

    def asdict(self) -> dict:
        d = {
            'columns': {},
            'relationships': {},
            'relationship_attrs': []
        }

        for column in SQLAlchemyTableAdapter.get_columns(self.item_class):
            d['columns'][column.name] = getattr(self.item, column.name)

        # for r in self.relationships:
        #     d['relationships'][r.name] = r.related_tables

        # for name in self.accepted_DOT_field_names:
        #     d['relationship_attrs'][name] = None

        return d


# Maybe make this all within an ItemAdapter
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

    @cached_property
    def primary_key(self):
        for column in self.columns:
            if (
                column.autoincrement is True
                or (
                    isinstance(column.type, Integer)
                    and column.primary_key is True
                    and column.foreign_keys == set()
                )
            ):
                return column

    @cached_property
    def unique_columns(self):
        pass

    @cached_property
    def foreign_keys(self):
        pass

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
