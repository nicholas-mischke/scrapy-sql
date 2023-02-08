
from collections import UserList
from scrapy.utils.misc import arg_to_iter
from scrapy.utils.python import flatten
from sqlalchemy.orm import Session


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

            instance = self.filter_instance(instance) # Insert Ignore 
            super().add(instance, _warn=True)

        super().commit()


class ScrapyBulkSession(Session):

    def __init__(self, declarative_base, autoflush=False, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.Base = declarative_base
        self.mapped_orm_classes = {
            x.entity.__table__: x.entity
            for x in self.Base.registry.mappers
        }

        self.metadata = self.Base.metadata
        self.sorted_tables = self.metadata.sorted_tables

        orm_sorted_tables = [
            self.mapped_orm_classes.get(tbl)
            for tbl in self.sorted_tables
        ]
        self.orm_sorted_tables = [
            orm_cls for orm_cls in orm_sorted_tables if orm_cls is not None
        ]  # Join tables can be part of sorted_tables, but aren't ORM classes

        self.instances = {
            orm_cls: UniqueList()
            for orm_cls in self.orm_sorted_tables
        }

    def add(self, instance):
        """
        Only adds DeclarativeMeta obj scraped directly from the spider.
        Does not "add" DeclarativeMeta obj within relationship attrs.
        """
        self.instances[instance.__class__].append(instance)

    def remove(self, instance):
        self.instances[instance.__class__].remove(instance)

    @property
    def flattened_instances(self):
        # Get a lst of lst from the self.instances dict
        # Flatten into a single list that preserves the sorted_tables
        # order of the metadata

        # return [
        #     instance for instances in self.instances.values()
        #     for instance in instances
        # ]

        return flatten(self.instances.values())

    @staticmethod
    def is_subdict(dict, possible_subdict):
        for k, v in possible_subdict.items():
            if dict.get(k) != v:
                return False
        return True

    def filter_instance(self, instance):
        # List of possible instances
        instances = self.instances.get(instance.__class__)
        instances_column_dicts = [
            instance.column_dict for instance in instances
        ]

        for i, dict in enumerate(instances_column_dicts):
            if ScrapyBulkSession.is_subdict(dict, instance.filter_kwargs):
                return instances[i]

        self.add(instance)
        return instance

    def commit(self):
        # Remove duplicate instances
        # These  duplicates are typically from partially loaded relationship attrs
        for orm_cls, instances in self.instances.items():
            for instance in instances:
                for relationship in instance.relationships:
                    if relationship.single_relation:
                        setattr(
                            instance,
                            relationship.name,
                            self.filter_instance(
                                relationship.related_instances
                            )
                        )
                    else:
                        setattr(
                            instance,
                            relationship.name,
                            UniqueList([
                                self.filter_instance(instance)
                                for instance in relationship.related_instances
                            ])
                        )

        for orm_cls, instances in self.instances.items():
            # When using subqueries it's stmt.values(lst_of_dicts)
            # Without using subqueries the lst_of_dicts is passed as
            # arg:`params` to self.execute
            if orm_cls.insert_upsert_uses_subqueries:
                self.execute(
                    orm_cls.insert_upsert_stmt.values(
                        [
                            instance.insert_upsert_dict
                            for instance in instances
                        ]
                    )
                )
            else:
                self.execute(
                    orm_cls.insert_upsert_stmt,
                    [
                        instance.insert_upsert_dict
                        for instance in instances
                    ]
                )
            super().commit()
