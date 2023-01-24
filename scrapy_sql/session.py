
from sqlalchemy.orm import Session, sessionmaker

from scrapy_sql import SQLAlchemyTableAdapter
from scrapy_sql._collections import ScrapyTableList


class ScrapySession(Session):

    def __init__(self, autoflush=False, *args, **kwargs):
        super().__init__(autoflush=autoflush, *args, **kwargs)

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
        super().add(
            self.filter_instance(instance),
            warn=_warn
        )

    def commit(self):
        self.resolve_relationships()
        super().commit()

    def resolve_relationships(self):
        for instance in self:

            adapter = SQLAlchemyTableAdapter(instance)

            for relationship in adapter.relationships:

                filtered_tables = ScrapyTableList(self)

                for filter_kwargs in adapter.filters.get(relationship.name):
                    filtered_table = self.filter_instance(
                        relationship.cls(**filter_kwargs)
                    )
                    filtered_tables.append(filtered_table)

                adapter[relationship.name] = filtered_tables

    def filter_instance(self, instance):
        """
        If the session already contains an instance,
        or an instance with identical primary_keys / unique_keys
        return the instance already in session, else return the instance
        """

        adapter = SQLAlchemyTableAdapter(instance)

        return self.query(
            instance.__class__
        ).filter_by(
            **adapter.filter_kwargs
        ).first() or instance


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
