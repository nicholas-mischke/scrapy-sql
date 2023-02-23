
# Project Imports
# from scrapy_sql.session import ScrapyBulkSession, ScrapyUnitOfWorkSession

# Scrapy / Twisted Imports
from scrapy import signals
from scrapy.extensions.feedexport import IFeedStorage, build_storage
from scrapy.exceptions import NotConfigured
from scrapy.utils.misc import load_object

from twisted.internet import threads

# SQLAlchemy Imports
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

# 3rd 🎉 Imports
from urllib.parse import urlparse
from zope.interface import implementer


def _default_commit(session):
    session.commit()


@implementer(IFeedStorage)
class SQLAlchemyFeedStorage:

    @classmethod
    def from_crawler(cls, crawler, uri, *, feed_options=None):

        declarative_base = feed_options.get('declarative_base')

        echo = (
            feed_options.get('engine_echo', False)
            or crawler.settings.get('SQLALCHEMY_ENGINE_ECHO', False)
        )

        sessionmaker_kwargs = (
            feed_options.get('sessionmaker_kwargs')
            or crawler.settings.get('SQLALCHEMY_SESSIONMAKER_KWARGS')
        )
        if 'class_' in sessionmaker_kwargs.keys():
            sessionmaker_kwargs['class_'] = load_object(
                sessionmaker_kwargs['class_']
            )
        Session = sessionmaker(**sessionmaker_kwargs)

        # Set-up event listeners
        session_events = feed_options.get('session_events', {})
        for event_name, func in session_events.items():
            event.listen(Session, event_name, load_object(func))

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

        obj = build_storage(
            cls,
            uri,
            echo=echo,
            declarative_base=load_object(declarative_base),
            Session=Session,
            commit=load_object(commit),
            feed_options=feed_options,
        )

        # Set signals for cls
        crawler.signals.connect(obj.close_spider, signals.spider_closed)

        return obj

    def __init__(
        self,
        uri,
        *,
        echo,
        declarative_base,
        Session,
        commit,
        feed_options=None
    ):
        self.uri = uri
        self.engine = create_engine(self.uri, echo=echo or True)

        # Create database/tables if they don't already exist
        declarative_base.metadata.create_all(self.engine)

        Session.config(self.engine)
        self.session = self.Session()

        self.commit = commit

    def open(self, spider):
        self.session.rollback()
        return self.session

    def store(self, session):
        if urlparse(self.uri).scheme == 'sqlite':  # SQLite is not thread safe
            self.commit(session)
        else:
            return threads.deferToThread(self.commit, session)

    def close_spider(self, spider):
        self.session.close()
        self.engine.dispose()
