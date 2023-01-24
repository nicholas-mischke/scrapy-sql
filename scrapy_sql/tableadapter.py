
# For Item Adapters
from sqlalchemy import Table, Integer
from sqlalchemy.orm.decl_api import DeclarativeMeta
from functools import cached_property

from itemadapter.adapter import AdapterInterface

from collections.abc import KeysView
from typing import Any, Iterator, Optional, List

from sqlalchemy.inspection import inspect

from sqlalchemy.orm.collections import InstrumentedList


class Relationship:
    def __init__(self, declarative_meta_instance, relationship):
        self.name = relationship.class_attribute.key
        self.cls = relationship.mapper.class_
        self.columns = self.cls.__table__.columns

        self.direction = relationship.direction.name
        self.related_tables = getattr(
            declarative_meta_instance,
            self.name
        )

        self.single_relation = not isinstance(
            self.related_tables,
            InstrumentedList
        )

    @cached_property
    def accepted_DOT_field_names(self):
        return tuple(
            f'{self.name}DOT{column.name}'
            for column in self.columns
        )

    def __len__(self):
        if self.single_relation:
            return 0 if self.related_tables is None else 1
        return len(self.related_tables)

    def __iter__(self):  # Iterate tables in relationship
        if self.single_relation:
            if self.related_tables is None:
                related_tables = []
            else:
                related_tables = [self.related_tables]
        else:
            related_tables = self.related_tables

        return iter(related_tables)


class RelationshipCollection:

    def __init__(self, declarative_meta_instance):

        self.relationships = tuple(
            Relationship(declarative_meta_instance, r)
            for r in inspect(declarative_meta_instance.__class__).relationships
        )
        self.relationships_dict = {
            r.name: r for r in self.relationships
        }

    @cached_property
    def names(self):
        return tuple(r.name for r in self.relationships)

    @cached_property
    def accepted_DOT_field_names(self):
        result = tuple()
        for relationship in self:
            result += relationship.accepted_DOT_field_names
        return result

    def __getitem__(self, relationship_name):
        return self.relationships_dict[relationship_name]

    def __iter__(self):
        return iter(self.relationships)


class _MixinColumnSQLAlchemyAdapter:

    @property
    def _fields_dict(self) -> dict:
        return self.asdict()

    def __getitem__(self, field_name: str) -> Any:
        return self.asdict()[field_name]

    def __setitem__(self, field_name: str, value: Any) -> None:
        # Column names can be sat directly
        if field_name in self.column_names:
            pass

        # Relationships can be sat directly
        elif field_name in self.relationships.names:
            relationship = self.relationships[field_name]

            if relationship.single_relation:
                if isinstance(value, relationship.cls):
                    pass
                elif (
                    hasattr(value, '__iter__')
                    and len(value) == 1
                    and isinstance(value[0], relationship.cls)
                ):
                    value = value[0]
                elif (
                    hasattr(value, '__iter__')
                    and len(value) == 0
                ):
                    value = None
                else:
                    raise TypeError
            else:
                if isinstance(value, relationship.cls):
                    value = [value]
                elif (
                    hasattr(value, '__iter__')
                    and all([isinstance(v, relationship.cls) for v in value])
                ):
                    pass
                else:
                    raise TypeError

        # This works for now, but needs to be update to support multiple fields
        elif field_name in self.relationships.accepted_DOT_field_names:
            relationship_name, relationship_column = field_name.split('DOT')
            relationship = self.relationships[relationship_name]

            field_name = f'{relationship_name}QUERY'
            value = relationship.cls(**{relationship_column: value})
        else:
            raise KeyError(
                f"{self.item_class_name} does not support field: {field_name}"
            )

        setattr(self.item, field_name, value)

    def __delitem__(self, field_name: str) -> None:
        """
        Can't or shouldn't delete a column from a SQLAlchemy Table.
        This needs to be defined for scrapy to work.
        """
        return None

    def __iter__(self) -> Iterator:
        return iter(self.asdict())

    def __len__(self) -> int:
        return len(self.columns)


class SQLAlchemyTableAdapter(_MixinColumnSQLAlchemyAdapter, AdapterInterface):
    """
    https://github.com/scrapy/itemadapter#extending-itemadapter
    """

    accepted_classes = (DeclarativeMeta, Table)

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

    def asdict(self):
        d = {}

        for column in self.columns:
            d[column.name] = getattr(self.item, column.name)

        # for r in self.relationships:
        #     d[r.name] = r.related_tables

        for name in self.relationships.accepted_DOT_field_names:
            d[name] = None

        return d

    @cached_property
    def columns(self):
        return self.item.__table__.columns

    @cached_property
    def column_names(self):
        return tuple(c.name for c in self.item.__table__.columns)

    @property
    def relationships(self):
        return RelationshipCollection(self.item)

    @property
    def filter_kwargs(self):
        d = {
            column.name: getattr(self.item, column.name)
            for column in self.columns
        }

        return {k: v for k, v in d.items() if v is not None}

        # # Query based on primary-key and unique keys
        # for column in self.columns:
        #     is_primary_key = column.primary_key
        #     is_autoincrement = (
        #         column.autoincrement is True
        #         or (
        #             isinstance(column.type, Integer)
        #             and column.primary_key is True
        #             and column.foreign_keys == set()
        #         )
        #     )
        #     is_unique = column.unique

        #     column_value = getattr(self.item, column.name)

        # return d


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

    pass

    # @property
    # def query_filter(self):
    #     """
    #     Mainly used by session.query(class).filter_by().first() calls
    #     """
    #     d = {}

    #     for column in self.columns:
    #         # # SQLite doesn't support DATE/TIME types, but instead converts them to strings.
    #         # # Without this block of code this fact creates problems when
    #         # # querying a session obj.
    #         # # https://gist.github.com/danielthiel/8374607
    #         # if isinstance(column.type, (DATE, Date)):
    #         #     d[column.name] = func.DATE(column)
    #         # elif isinstance(column.type, (DATETIME, DateTime)):
    #         #     d[column.name] = func.DATETIME(column)
    #         # elif isinstance(column.type, (TIME, Time)):
    #         #     d[column.name] = func.TIME(column)
    #         # elif isinstance(column.type, TIMESTAMP):
    #         #     d[column.name] = func.TIMESTAMP(column)

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

    # def asdict(self):
    #     d = {}

    #     for column in self.columns:
    #         d[column.name] = getattr(self, column.name)

    #     for r in self.relationships:
    #         d[r.name] = r.related_tables

    #     for name in self.accepted_DOT_field_names:
    #         d[name] = None

    #     return d

    def __repr__(self):
        result = f"{self.__class__.__name__}("

        for column in self.__table__.columns:
            column_value = getattr(self, column.name)
            if isinstance(column_value, str):
                result += f"{column.name}='{column_value}', "
            else:  # No quotation marks if not a string
                result += f"{column.name}={column_value}, "

        # for relationship in self.relationships:
        #     result += f"{relationship.name}={relationship.related_tables},"

        return result.strip(", ") + ")"

    __str__ = __repr__
