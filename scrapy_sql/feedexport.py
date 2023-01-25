
# Project Imports
from scrapy_sql.session import ScrapySession

# Scrapy / Twisted Imports
from scrapy.extensions.feedexport import IFeedStorage, build_storage
from scrapy.exceptions import NotConfigured
from scrapy.utils.misc import load_object

from twisted.internet import threads

# SQLAlchemy Imports
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# 3rd 🎉 Imports
from urllib.parse import urlparse
from zope.interface import implementer


def _default_commit(session):
    session.commit()
    session.close()


@implementer(IFeedStorage)
class SQLAlchemyFeedStorage:

    @classmethod
    def from_crawler(cls, crawler, uri, *, feed_options=None):

        try:
            item_classes = feed_options['item_classes']
            metadata = load_object(item_classes[0]).metadata
        except KeyError:
            raise NotConfigured

        if (
            feed_options.get('engine_echo', False)
            or crawler.settings.get('SQLALCHEMY_ENGINE_ECHO', False)
        ) is True:
            echo = True
        else:
            echo = False

        sessionmaker_kwargs = (
            feed_options.get('sessionmaker_kwargs')
            or crawler.settings.get('SQLALCHEMY_SESSIONMAKER_KWARGS')
            or {'class_': ScrapySession, 'metadata': metadata}
        )

        commit = (
            feed_options.get('sqlalchemy_commit')
            or crawler.settings.get('SQLALCHEMY_COMMIT')
            or _default_commit
        )

        # Allows multiple options for settings the SQLALCHEMY_ADD constant
        # While we have access to the crawler obj
        feed_options.setdefault('item_export_kwargs', {})
        add = (
            feed_options.get('item_export_kwargs').get('sqlalchemy_add')
            or feed_options.get('sqlalchemy_add')
            or crawler.settings.get('SQLALCHEMY_ADD')
        )
        feed_options.get('item_export_kwargs').setdefault(
            'sqlalchemy_add', add)

        return build_storage(
            cls,
            uri,
            metadata=metadata,
            echo=echo,
            sessionmaker_kwargs=sessionmaker_kwargs,
            commit=load_object(commit),
            feed_options=feed_options,
        )

    def __init__(
        self,
        uri,
        *,
        metadata,
        echo,
        sessionmaker_kwargs,
        commit,
        feed_options=None
    ):
        self.uri = uri
        self.engine = create_engine(self.uri, echo=echo)

        # Create database if it doesn't already exist
        self.metadata = metadata
        self.metadata.create_all(self.engine)

        sessionmaker_kwargs.setdefault('bind', self.engine)
        self.sessionmaker_kwargs = sessionmaker_kwargs

        self.commit = commit

    def open(self, spider):
        return sessionmaker(**self.sessionmaker_kwargs)()

    def store(self, session):
        if urlparse(self.uri).scheme == 'sqlite':  # SQLite is not thread safe
            self.commit(session)
        else:
            return threads.deferToThread(self.commit, session)
