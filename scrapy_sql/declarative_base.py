# Scrapy / Twisted Imports
from scrapy.utils.misc import arg_to_iter

# SQLAlchemy Imports
from sqlalchemy import Integer, insert
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.collections import DeclarativeBase, InstrumentedList


# 3rd 🎉 Imports
from descriptors import cachedclassproperty, classproperty
from functools import cached_property


class Relationship:
    def __init__(self, instance, relationship):

        self.name = relationship.class_attribute.key

        self.cls = relationship.mapper.class_
        self.table_cls = self.cls.__table__
        self.tablename = self.cls.__tablename__

        self.columns = self.cls.__table__.columns
        self.direction = relationship.direction.name

        self.related_instance = getattr(
            instance,
            self.name
        )
        self.single_relation = not isinstance(
            self.related_instance,
            InstrumentedList
        )

    def __len__(self):
        return len(arg_to_iter(self.related_instance))

    def __iter__(self):  # Iterate tables in relationship
        return iter(arg_to_iter(self.related_instance))

    def __repr__(self):
        return str(self.__dict__)

    __str__ = __repr__


class ScrapyDeclarativeBase(DeclarativeBase):
    """
    Scrapy default logging of an item writes the item to the log file, in a
    format identical to str(my_dict).

    sqlalchemy.orm.decl_api.DeclarativeMeta classes use
    sqlalchemy.ext.declarative.declarative_base() classes as parent classes.

    If sqlalchemy.orm.decl_api.DeclarativeMeta classes instead have dual
    inheritence by adding this class Scrapy logging output can behave
    normally by utilizing this classes' __repr__ method
    """

    @cachedclassproperty
    def column_names(cls):
        # Called from get_field_names_from_class in ItemAdapter
        return tuple(c.name for c in cls.__table__.columns)

    @cachedclassproperty
    def relationship_names(cls):
        return tuple(
            Relationship(cls(), relationship).name
            for relationship in inspect(cls).relationships
        )

    @cached_property
    def columns(self):
        return self.__table__.columns

    @property
    def relationships(self):
        return tuple(
            Relationship(self, relationship)
            for relationship in inspect(self.__class__).relationships
        )

    @property
    def filter_kwargs(self):
        d = {}

        for column in self.columns:

            column_value = getattr(self, column.name)

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

            # Don't include NoneType in filter values
            elif column_value is not None:
                [column.name] = column_value

        return d

    @classproperty
    def stmt(cls):
        return insert(cls).prefix_with('OR IGNORE')

    @property
    def stmt_values(self):
        pass

    def __repr__(self):
        result = f"{self.__class__.__name__}("

        for column in self.columns:
            column_value = getattr(self, column.name)
            if isinstance(column_value, str):
                result += f"{column.name}='{column_value}', "
            else:  # No quotation marks if not a string
                result += f"{column.name}={column_value}, "

        for r in self.relationships:
            result += f"{r.name}={r.related_instance}, "

        return result.strip(", ") + ")"

    __str__ = __repr__

    def __eq__(self, other):

        if not (
            isinstance(other, type(self))
            and isinstance(self, type(other))
        ):
            return False

        for column in self.columns:
            if getattr(self, column.name) != getattr(other, column.name):
                return False

        return True

    @classmethod
    def from_log_file(cls, string):
        pass

    @classproperty
    def mapped_orm_classes(cls):
        return tuple(cls.registry.mappers)

    @classproperty
    def sorted_tables(cls):
        return cls.metadata.sorted_tables

    @classproperty
    def sorted_orm_classes(cls):
        d = {
            x.__table__: x
            for x in cls.mapped_orm_classes
        }
        return tuple(
            d.get(tbl) for tbl in cls.sorted_tables
        )
