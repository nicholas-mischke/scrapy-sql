
from sqlalchemy import create_engine
from scrapy_sql.scrapy_session import ScrapySession

from scrapy.extensions.feedexport import IFeedStorage, build_storage
from scrapy.exceptions import NotConfigured
from scrapy.utils.misc import load_object

from twisted.internet import threads
from urllib.parse import urlparse
from zope.interface import implementer


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

        try:
            item_classes = feed_options['item_classes']
        except KeyError:
            raise NotConfigured

        return build_storage(
            cls,
            uri,
            metadata=load_object(item_classes[0]).metadata,
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

    def open(self, spider):
        return ScrapySession(self.engine)

    def store(self, session):
        if urlparse(self.uri).scheme == 'sqlite':  # SQLite is not thread safe
            self.commit(session)
        else:
            return threads.deferToThread(self.commit, session)
