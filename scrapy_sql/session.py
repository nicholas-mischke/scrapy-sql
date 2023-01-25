
from sqlalchemy.orm import Session, sessionmaker

from scrapy.utils.misc import arg_to_iter
from scrapy_sql import SQLAlchemyTableAdapter

from collections import UserList


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

    def extend(self, iter):
        for obj in arg_to_iter(iter):
            self.append(obj)


class ScrapySession(Session):

    def __init__(self, metadata, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.instances = {
            table_cls: UniqueList()
            for table_cls in metadata.sorted_tables
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

        for table_cls, instances in self.instances.items():
            for instance in instances:

                for relationship in instance.relationships:
                    if relationship.single_relation:
                        setattr(
                            instance,
                            relationship.name,
                            self.filter_instance(relationship.related_tables)
                        )
                    else:
                        setattr(
                            instance,
                            relationship.name,
                            UniqueList(
                                [self.filter_instance(i) for i in relationship]
                            )
                        )

                super().add(instance, _warn=True)

            super().commit()
