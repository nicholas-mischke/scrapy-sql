
from sqlalchemy.orm import sessionmaker
from scrapy_sql._collections import ScrapyTableList
from scrapy_sql import SQLAlchemyTableAdapter


class ScrapySession:

    def __init__(self, engine):
        Session = sessionmaker()
        Session.configure(bind=engine)
        self.session = Session()

        # Used to resolve relationships later
        # Difficult to determine cls types held in session objects
        # Also dosen't really allow you to iterate tables in session
        self.added_tables = []

    def add(self, table):

        adapter = SQLAlchemyTableAdapter(table)

        # Make sure to not add duplicates via relationships
        for relationship in adapter.relationships:
            adapter[relationship.name] = ScrapyTableList(
                relationship.related_tables)

            filtered_tables = ScrapyTableList(self)

            for related_table in relationship:
                filtered_tables.append(related_table)

            adapter[relationship.name] = filtered_tables

        # Don't add duplicates to session obj
        table = self.query_session(table)

        if table not in self.added_tables:
            self.added_tables.append(table)

        self.session.add(table)

    def resolve_relationships(self):
        for table in self.added_tables:
            for relationship in table.relationships:

                query_filter = table.query_filters.get(relationship.name)

                if query_filter.isempty:
                    continue

                filtered_tables = ScrapyTableList(self)
                for filter_kwargs in query_filter:

                    table = self.query_session(
                        relationship.cls(**filter_kwargs)
                    )
                    filtered_tables.append(table)

                relationship.replace_tables(filtered_tables)

    def query_session(self, table):
        return self.session.query(
            table.__class__
        ).filter_by(
            **table.query_filter
        ).first() or table
