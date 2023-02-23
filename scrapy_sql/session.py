
from collections import UserList

from scrapy.utils.misc import arg_to_iter
from scrapy.utils.python import flatten

from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Session
from sqlalchemy.orm.collections import InstrumentedList


# if relationship.direction is MANYTOMANY
# from sqlalchemy.interfaces import MANYTOMANY
# from sqlalchemy.interfaces import MANYTOONE
# from sqlalchemy.interfaces import ONETOMANY


class ScrapyNoDuplicateSession(Session):

    def filter_instance(self, instance):
        # dirty = self._dirty_states or self.dirty (instances)
        # deleted = set(self._deleted)
        # new = set(self._new)

        filter_kwargs = {}
        for column in instance.__class__.__table__.columns:
            column_value = getattr(instance, column.name)

            if column.foreign_keys != set():
                continue

            if column_value is None:
                continue

            filter_kwargs[column.name] = column_value

        return self.query(
            instance.__class__
        ).filter_by(
            **filter_kwargs
        ).first() or instance

    def add(self, instance, _warn=True):

        # for relationship in inspect(instance.__class__).relationships:
        #     relationship_name = relationship.class_attribute.key
        #     related_instances = getattr(instance, relationship_name)

        #     if related_instances is None or len(related_instances) == 0:
        #         continue

        #     if isinstance(related_instances, InstrumentedList):
        #         filtered_related_instances = InstrumentedList()
        #         for related_instance in related_instances:
        #             filtered_related_instances.append(
        #                 self.filter_instance(related_instance)
        #             )
        #     else:
        #         filtered_related_instances = self.filter_instance(
        #             related_instances
        #         )

        #     setattr(instance, relationship_name, filtered_related_instances)

        # instance = self.filter_instance(instance)  # Insert Ignore
        super().add(instance, _warn=_warn)


class UniqueList(UserList):
    """
    https://stackoverflow.com/questions/6654613/what-is-an-instrumentedlist-in-python

    SQLAlchemy uses an InstrumentedList as list-like object
    which is aware of insertions and deletions of related objects to an object
    (via one-to-many and many-to-many relationships).

    ScrapyInstrumentedList doesn't allow duplicate tables, or tables that'd
    cause confilct with primary key / unique keys
    """

    def __init__(self, *args, **kwargs):

        # Making a list via list comprehension
        if (
            len(args) == 1
            and hasattr(args[0], '__iter__')
        ):
            super().__init__(*tuple(), **kwargs)

            for obj in args[0]:
                if obj not in self.data:
                    super().append(obj)
        else:
            super().__init__(*args, **kwargs)

    def append(self, obj):
        if obj not in self.data:  # No duplicates
            super().append(obj)

        # try:
        #     index = self.data.index(obj)
        #     similar_obj = self.data[index]

        #     if obj > similar_obj:
        #         self.data[index] = obj
        # except ValueError:  # Not in self.data
        #     super().append(obj)

    def extend(self, iter):
        for obj in arg_to_iter(iter):
            self.append(obj)


class ScrapyUnitOfWorkSession(Session):

    def __init__(self, declarative_base, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.instances = {
            table_cls: UniqueList()
            for table_cls in declarative_base.metadata.sorted_tables
        }

    def add(self, instance):
        """
        Only adds DeclarativeMeta obj scraped directly from the spider.
        Does not "add" DeclarativeMeta obj within relationship attrs.
        """
        self.instances[instance.__class__.__table__].append(instance)

    def filter_instance(self, instance):
        return self.query(
            instance.__class__
        ).filter_by(
            **instance.filter_kwargs
        ).first() or instance

    def commit(self):

        # Flattened list in sorted_tables order
        instances = [i for values in self.instances.values() for i in values]

        for instance in instances:

            for relationship in instance.relationships:
                if relationship.single_relation:
                    setattr(
                        instance,
                        relationship.name,
                        self.filter_instance(relationship.related_instances)
                    )
                else:
                    setattr(
                        instance,
                        relationship.name,
                        UniqueList(
                            [self.filter_instance(i) for i in relationship]
                        )
                    )

            instance = self.filter_instance(instance)  # Insert Ignore
            super().add(instance, _warn=True)

        super().commit()

