
# Project Imports
from scrapy_sql.session import ScrapyBulkSession
import scrapy_sql.utils as utils

# Scrapy / Twisted Imports
from scrapy import signals
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


class SQLAlchemyInstanceFilter:
    def __init__(self, feed_options):
        self.feed_options = feed_options
        self.Base = load_object(self.feed_options['declarative_base'])
        self.instance_classes = tuple(self.Base.sorted_entities)

    def accepts(self, instance):
        return isinstance(instance, self.instance_classes)


def _default_commit(session):
    if isinstance(session, ScrapyBulkSession):
        session.bulk_commit()
    else:
        session.commit()


@implementer(IFeedStorage)
class SQLAlchemyFeedStorage:

    @classmethod
    def from_crawler(cls, crawler, uri, *, feed_options=None):

        # Settings priorities
        # 1) feed_options['item_export_kwargs'] (when applicable)
        # 2) feed_options
        # 3) crawler.settings

        feed_options.setdefault('item_export_kwargs', {})

        Base = load_object(
            feed_options.get('declarative_base')
            or crawler.settings.get('SQLALCHEMY_DECLARATIVE_BASE')
        )
        if not Base:
            raise NotConfigured()

        echo = (
            feed_options.get('engine_echo', False)
            or crawler.settings.get('SQLALCHEMY_ENGINE_ECHO', False)
        )

        sessionmaker_kwargs = (
            feed_options.get('sessionmaker_kwargs')
            or crawler.settings.getdict('SQLALCHEMY_SESSIONMAKER_KWARGS')
            or {'class_': ScrapyBulkSession}
        )

        add = (
            feed_options.get('item_export_kwargs').get('add')
            or feed_options.get('add')
            or crawler.settings.get('SQLALCHEMY_ADD')
        )

        commit = load_object(
            feed_options.get('commit')
            or crawler.settings.get('SQLALCHEMY_COMMIT')
            or _default_commit
        )

        # ORM INSERTs / UPSERTs are sat with the following priorities
        # 1) feed_options
        # 2) as a classproperty of the entity or table
        # 3) crawler.settings
        # 4) sqlalchemy.insert(table)

        default_orm_stmt = load_object(
            crawler.settings.get('SQLALCHEMY_DEFAULT_STMT')
            or utils.insert
        )

        orm_stmts = {}

        for table in Base.sorted_tables:
            try:
                # for DeclarativeBase subclasses a stmt classproperty
                # can be added to the class declaration

                # for core table objs (or DeclarativeBase subclasses)
                # setattr(table, 'stmt', orm_stmt)
                # can be placed after the class declaration
                stmt = table.stmt
            except AttributeError:
                stmt = default_orm_stmt(table)
            orm_stmts[table] = stmt

        # feed_options take priority
        for table, stmt in feed_options.get('orm_stmts', {}).items():

            if isinstance(table, str):
                table = load_object(table)

            try:
                table = table.__table__  # orm entity
            except AttributeError:  # core table obj
                pass

            if isinstance(stmt, str):
                stmt = load_object(stmt)

            try:
                stmt = stmt(table)
            except TypeError:  # 'Insert' object is not callable
                stmt = stmt

            orm_stmts[table] = stmt  # insert(table)

        # Adjust feed_options for loaded items
        feed_options['declarative_base'] = Base
        feed_options['echo'] = echo
        feed_options['sessionmaker_kwargs'] = sessionmaker_kwargs
        feed_options['add'] = add
        feed_options['item_export_kwargs']['add'] = add
        feed_options['commit'] = commit
        feed_options['orm_stmts'] = orm_stmts

        obj = build_storage(
            cls,
            uri,
            feed_options=feed_options,
        )

        # Set signals for cls
        crawler.signals.connect(obj.close_spider, signals.spider_closed)

        return obj

    def __init__(
        self,
        uri,
        *,
        feed_options=None
    ):
        self.uri = uri
        self.echo = feed_options.get('echo')
        self.Base = feed_options.get('declarative_base')
        self.sessionmaker_kwargs = feed_options.get('sessionmaker_kwargs')
        self.commit = feed_options.get('commit')

        # Set sessionmaker_kwargs
        # This needs a try/except block because this class gets initalized twice by Scrapy
        if 'bind' in self.sessionmaker_kwargs.keys():
            self.engine = self.sessionmaker_kwargs['bind']
            if isinstance(self.engine, str):
                self.engine = load_object(self.sessionmaker_kwargs['bind'])
        else:
            self.engine = create_engine(
                self.uri,
                echo=self.echo
            )
            self.sessionmaker_kwargs['bind'] = self.engine

        if isinstance(self.sessionmaker_kwargs['class_'], str):
            self.sessionmaker_kwargs['class_'] = load_object(
                self.sessionmaker_kwargs['class_']
            )

        if self.sessionmaker_kwargs.get('class_') in (ScrapyBulkSession, ):
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
