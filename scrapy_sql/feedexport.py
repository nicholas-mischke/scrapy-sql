
from scrapy_sql.tableadapter import SQLAlchemyTableAdapter
from scrapy_sql.scrapy_session import ScrapySession

from sqlalchemy.orm import sessionmaker

from scrapy.extensions.feedexport import IFeedStorage, build_storage
from scrapy.utils.misc import load_object

from twisted.internet import threads
from urllib.parse import urlparse
from zope.interface import implementer
from sqlalchemy.inspection import inspect
from pprint import pprint

from sqlalchemy import create_engine
from scrapy.exceptions import NotConfigured


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


def _default_commit(scrapy_session):
    """
    resolve one-to-many relationships etc.
    """
    scrapy_session.resolve_relationships()

    session = scrapy_session.session

    session.commit()
    session.close()


@implementer(IFeedStorage)
class SQLAlchemyFeedStorage:

    @classmethod
    def from_crawler(cls, crawler, uri, *, feed_options=None):

        item_classes = feed_options.get('item_classes')
        if item_classes is None:
            raise NotConfigured
        else:
            metadata = load_object(item_classes[0]).metadata

        return build_storage(
            cls,
            uri,
            metadata=metadata,
            commit=load_object(
                feed_options.get('sqlalchemy_commit')
                or crawler.settings.get('SQLALCHEMY_COMMIT')
                or _default_commit
            ),
            feed_options=feed_options,
        )

    def __init__(self, uri, *, metadata, commit, feed_options=None):
        self.uri = uri

        self.engine = create_engine(self.uri)

        # Create database if it doesn't already exist
        metadata.create_all(self.engine)

        self.commit = commit
        self.feed_options = feed_options

    def open(self, spider):
        return ScrapySession(self.engine)

    def store(self, session):
        if urlparse(self.uri).scheme == 'sqlite':  # SQLite is not thread safe
            self.commit(session)
        else:
            return threads.deferToThread(self.commit, session)
