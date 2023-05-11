
# Project Imports
from scrapy_sql._defaults import (
    _default_add,
    _default_commit,
    _default_insert
)
from scrapy_sql.session import ScrapyBulkSession
from scrapy_sql.utils import load_table, load_stmt

# Scrapy / Twisted Imports
from scrapy import signals
from scrapy.extensions.feedexport import IFeedStorage, build_storage
from scrapy.exceptions import NotConfigured
from scrapy.utils.misc import load_object
from scrapy.utils.python import get_func_args

from twisted.internet import threads

# SQLAlchemy Imports
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

# 3rd 🎉 Imports
from urllib.parse import urlparse
from zope.interface import implementer


class SQLAlchemyInstanceFilter:

    def __init__(self, feed_options):
        self.feed_options = feed_options
        # required, so don't use `get` method
        self.Base = load_object(self.feed_options['declarative_base'])
        self.instance_classes = tuple(self.Base.sorted_entities)

    def accepts(self, instance):
        return isinstance(instance, self.instance_classes)


@implementer(IFeedStorage)
class SQLAlchemyFeedStorage:

    @classmethod
    def from_crawler(cls, crawler, uri, *, feed_options=None):
        # Settings priorities
        # 1) feed_options['item_export_kwargs'] (when applicable)
        # 2) feed_options
        # 3) crawler.settings

        feed_options.setdefault(
            'declarative_base',
            crawler.settings.get('SQLALCHEMY_DECLARATIVE_BASE')
        )
        if feed_options.get('declarative_base') is None:
            raise NotConfigured()

        feed_options.setdefault(
            'engine_echo',
            crawler.settings.get('SQLALCHEMY_ENGINE_ECHO', False)
        )

        feed_options.setdefault(
            'sessionmaker_kwargs',
            (
                crawler.settings.getdict('SQLALCHEMY_SESSIONMAKER_KWARGS')
                or {'class_': ScrapyBulkSession}
            )
        )
        # Even though this is the default, it'll make it easier in the
        # __init__ method to make this explicit
        feed_options.get('sessionmaker_kwargs').setdefault('class_', Session)

        # set up orm_stmts
        feed_options.setdefault('orm_stmts', {})
        orm_stmts = feed_options.get('orm_stmts')

        # Determine custom set values first
        orm_stmts = {
            load_table(table): load_stmt(stmt)
            for table, stmt in orm_stmts.items()
        }

        # If it isn't custom, set default
        default_stmt = load_object(
            crawler.settings.get('SQLALCHEMY_DEFAULT_ORM_STMT')
            or _default_insert
        )

        Base = load_object(feed_options.get('declarative_base'))
        sorted_tables = Base.metadata.sorted_tables

        for table in sorted_tables:
            orm_stmts.setdefault(
                load_table(table),
                load_stmt(default_stmt)
            )

        feed_options['orm_stmts'] = orm_stmts

        # set add in both feed_options and item_export_kwargs
        feed_options.setdefault('item_export_kwargs', {})
        add = (
            feed_options.get('item_export_kwargs').get('add')
            or feed_options.get('add')
            or crawler.settings.get('SQLALCHEMY_ADD')
            or _default_add
        )
        feed_options['add'] = add
        feed_options['item_export_kwargs']['add'] = add

        feed_options.setdefault(
            'commit',
            (
                crawler.settings.get('SQLALCHEMY_COMMIT')
                or _default_commit
            )
        )

        obj = build_storage(
            cls,
            uri,
            feed_options=feed_options,
        )

        # Set signals for cls
        # crawler.signals.connect(obj.close_spider, signals.spider_closed)

        return obj

    def __init__(
        self,
        uri,
        *,
        feed_options=None
    ):
        self.uri = uri
        self.feed_options = feed_options

        self.Base = load_object(feed_options.get('declarative_base'))
        self.commit = load_object(feed_options.get('commit'))
        # sessionmaker_kwargs keys: bind, class_, autoflush, expire_on_commit, info
        self.sessionmaker_kwargs = feed_options.get('sessionmaker_kwargs')

        self.engine = create_engine(self.uri, echo=feed_options.get('echo'))
        self.sessionmaker_kwargs['bind'] = self.engine

        session_cls = load_object(self.sessionmaker_kwargs['class_'])
        self.sessionmaker_kwargs['class_'] = session_cls
        if 'feed_options' in get_func_args(session_cls):
            self.sessionmaker_kwargs['feed_options'] = feed_options

        self.Session = sessionmaker(**self.sessionmaker_kwargs)
        self.session = self.Session()

        # Create database/tables if they don't already exist
        self.Base.metadata.create_all(self.engine)

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
