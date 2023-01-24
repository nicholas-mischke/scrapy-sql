
from sqlalchemy import Table
from sqlalchemy.orm.decl_api import DeclarativeMeta

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

    accepted_classes = (DeclarativeMeta, Table)

    def __init__(self, session, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = session

    def append(self, table):

        if not isinstance(table, ScrapyTableList.accepted_classes):
            raise TypeError()

        # Don't allow for integrety exceptions to be raised
        table = self.session.query_session(table)

        if table not in self.data:  # No duplicates
            super().append(table)
