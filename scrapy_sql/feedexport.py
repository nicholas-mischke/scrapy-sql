
from scrapy_sql.connections import base_engine_mapping
from scrapy_sql.tableadapter import SQLAlchemyTableAdapter

from sqlalchemy.orm import sessionmaker

from scrapy.extensions.feedexport import IFeedStorage, build_storage
from scrapy.utils.misc import load_object

from twisted.internet import threads
from urllib.parse import urlparse
from zope.interface import implementer
from sqlalchemy.inspection import inspect
from pprint import pprint


def print_dir_obj(obj, tab_count=0):
    print(f"\n\n{'    '*tab_count}{obj.__class__}")
    print(f"{'    '*tab_count}{obj}\n\n")
    for attr in dir(obj):
        try:
            obj_attr = getattr(obj, attr)
            # if hasattr(obj_attr, '__call__'):
            #     print_dir_obj(obj_attr, tab_count=tab_count+1)
            #     print()
            # else:
            if attr == '__dict__':
                print(f"{'    '*tab_count}{attr}: ")
                pprint(obj_attr)
            else:
                print(f"{'    '*tab_count}{attr} : {obj_attr}")
            print()
        except AttributeError:
            print(f"{'    '*tab_count}{attr}: AttributeError")
    input()


def _default_commit(session):
    """
    resolve one-to-many relationships etc.
    """

    for cls in SQLAlchemyTableAdapter.seen_classes:
        for table in session.query(cls).all():

            if 'quotes.tables.Quote' in str(cls):
                print('\n\n_default_commit')
                print(table)
                print(f"{table.query_filters=}")
                input('\n\n')

            table.query_relationships(session)
            table.filter_relationships(session)

            print()

    session.commit()
    session.close()

    # for cls in SQLAlchemyTableAdapter.seen_classes:
    #     if not 'quotes.tables.Quote' in str(cls):
    #         continue

    #     for tbl in session.query(cls).all():
    #         if tbl.quote == 'If not us, who? If not now, when?':
    #             print_dir_obj(tbl)
    #     input()

    # # # In the open method of the SQLAlchemyFeedStorage cls
    # # # we configure the session with Session.configure(binds=base_engine_mapping)
    # # # To get all tables in the session, we need the metadata of the Base
    # # base_engine_mapping = session._Session__binds
    # # engines = list(base_engine_mapping.values())
    # # Bases = list(base_engine_mapping.keys())
    # # metadata_lst = [Base.metadata for Base in Bases]
    # #     # metadata.sorted_tables
    # #     # Base.registry.mapped
    # #     # Base.registry._class_registry.data.values()


@implementer(IFeedStorage)
class SQLAlchemyFeedStorage:

    @classmethod
    def from_crawler(cls, crawler, uri, *, feed_options=None):
        return build_storage(
            cls,
            uri,
            commit=load_object(
                feed_options.get('sqlalchemy_commit')
                or crawler.settings.get('SQLALCHEMY_COMMIT')
                or _default_commit
            ),
            feed_options=feed_options,
        )

    def __init__(self, uri, *, commit=None, feed_options=None):
        self.uri = uri
        self.commit = commit
        self.feed_options = feed_options

    def open(self, spider):
        Session = sessionmaker()
        Session.configure(binds=base_engine_mapping)
        return Session(no_autoflush=True)

    def store(self, session):
        if urlparse(self.uri).scheme == 'sqlite':  # SQLite is not thread safe
            self.commit(session)
        else:
            return threads.deferToThread(self.commit, session)
