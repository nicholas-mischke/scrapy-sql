
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.collections import InstrumentedList


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
        # Make sure to not add duplicates via relationships
        for relationship in table.relationships:
            filtered_tables = InstrumentedList()

            for related_table in relationship:
                filtered_tables.append(self.query_session(related_table))

            relationship.replace_tables(filtered_tables)

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

                filtered_tables = InstrumentedList()
                for filter_kwargs in query_filter:
                    filtered_tables.append(
                        self.query_session(
                            filter_kwargs,
                            relationship.cls
                        )
                    )
                relationship.replace_tables(filtered_tables)

    def query_session(self, table, filter_cls=None):

        if isinstance(table, dict):
            filter_kwargs = table
            if filter_cls is None:  # Cannot filter out based on <class dict> belwo
                raise BaseException()
        else:
            filter_kwargs = table.query_filter

        if filter_cls is None:
            filter_cls = table.__class__

        return self.session.query(
            filter_cls
        ).filter_by(
            **filter_kwargs
        ).first() or table
