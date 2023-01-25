
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

    def __init__(self, metadata, autoflush=False, *args, **kwargs):

        super().__init__(autoflush=autoflush, *args, **kwargs)

        self.metadata = metadata
        self.sorted_tables = self.metadata.sorted_tables

        self.instances = {
            table_cls: UniqueList()
            for table_cls in self.sorted_tables
        }

    def add(self, instance):
        """
        Only adds DeclarativeMeta obj scraped directly from the spider.
        Does not "add" DeclarativeMeta obj within relationship attrs.
        """
        self.instances[instance.__class__.__table__].append(instance)

    def resolve_potential_IntegrityErrors(self):

        for table_cls, instances in self.instances.items():
            print(f"{table_cls=}")
            for i, instance in enumerate(instances):
                print('------------------------------',
                      i, '------------------------------')
                print(instance)

                adapter = SQLAlchemyTableAdapter(instance)

                for relationship in adapter.relationships:
                    print('    ', relationship.name, ':')

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
                if adapter.relationships:
                    super().commit()
                    print(instance)

            super().commit()
            print('\nNow in Session')
            for _ in self:
                print(_)
            print('\n\n')

    def filter_instance(self, instance):
        adapter = SQLAlchemyTableAdapter(instance)

        exists = self.query(
            instance.__class__
        ).filter_by(
            **adapter.filter_kwargs
        ).first()

        print(f"        {instance=}")
        print(f"        {adapter.filter_kwargs=}")
        print(f"        {exists=}")
        print(f"        {exists==instance=}")
        print()

        return exists if exists is not None else instance

    def commit(self):
        self.resolve_potential_IntegrityErrors()
        super().commit()


class scrapy_sessionmaker(sessionmaker):

    def __init__(
        self,
        class_=ScrapySession,
        autoflush=False,
        *args,
        **kwargs
    ):
        super().__init__(
            class_=class_,
            autoflush=autoflush,
            *args,
            **kwargs
        )


if __name__ == '__main__':

    lst = ScrapyUniqueList(['hi', 'hello', 'hi'])
    print(lst)

    lst = ScrapyUniqueList([5, 7, 5])
    print(lst)
