
from sqlalchemy.orm import Session, sessionmaker

from scrapy_sql import SQLAlchemyTableAdapter

from collections import UserList


class ScrapyTableList(UserList):
    """
    https://stackoverflow.com/questions/6654613/what-is-an-instrumentedlist-in-python

    SQLAlchemy uses an InstrumentedList as list-like object
    which is aware of insertions and deletions of related objects to an object
    (via one-to-many and many-to-many relationships).

    ScrapyInstrumentedList doesn't allow duplicate tables, or tables that'd
    cause confilct with primary key / unique keys
    """

    def __init__(self, session, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = session

    def append(self, table):
        # Don't allow for integrety exceptions to be raised
        table = self.session.filter_instance(table)

        if table not in self.data:  # No duplicates
            super().append(table)


class ScrapySession(Session):

    def __init__(self, autoflush=False, *args, **kwargs):
        super().__init__(autoflush=autoflush, *args, **kwargs)
        self.instances = []

    def add(self, instance, _warn=True):

        adapter = SQLAlchemyTableAdapter(instance)

        # Make sure to not add duplicates via relationships
        for relationship in adapter.relationships:

            filtered_tables = ScrapyTableList(self)

            for related_table in relationship:
                filtered_table = self.filter_instance(related_table)
                filtered_tables.append(filtered_table)

            adapter[relationship.name] = filtered_tables

        # Don't add duplicates to self
        instance = self.filter_instance(instance)

        if instance not in self.instances:
            self.instances.append(instance)

        super().add(instance, _warn)

    def commit(self):
        self.resolve_relationships()
        super().commit()

    def resolve_relationships(self):
        for instance in self.instances:

            adapter = SQLAlchemyTableAdapter(instance)

            for relationship in adapter.relationships:
                try:
                    query_table = getattr(
                        instance,
                        f'{relationship.name}QUERY'
                    )
                except AttributeError:
                    continue

                adapter[relationship.name] = self.filter_instance(query_table)

    def filter_instance(self, instance):
        """
        If the session already contains an instance,
        or an instance with identical primary_keys / unique_keys
        return the instance already in session, else return the instance

        This is meant to avoid raising sqlalchemy.exc.IntegrityError
        """

        # Don't use adapter, just determine filter_kwargs here???
        adapter = SQLAlchemyTableAdapter(instance)

        exists = self.query(
            instance.__class__
        ).filter_by(
            **adapter.filter_kwargs
        ).first()

        return exists if exists is not None else instance


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
