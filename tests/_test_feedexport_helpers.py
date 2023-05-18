
from copy import copy, deepcopy

from scrapy_sql import ScrapyDeclarativeBase
from scrapy_sql._defaults import (
    _default_add,
    _default_commit,
    _default_insert
)
from scrapy_sql.session import ScrapyBulkSession

from sqlalchemy.orm import Session, DeclarativeBase
from sqlalchemy import Column, Integer, Table


# Defaults
class DefaultDeclarativeBase(DeclarativeBase, ScrapyDeclarativeBase):
    pass

class DefaultModel(DefaultDeclarativeBase):
    __tablename__ = 'default_table'
    id = Column(Integer, primary_key=True)

default_table = DefaultModel.__table__

default_settings_dict = {
    # Scrapy will issue a warning if this setting isn't sat
    # Set settings whose default value is deprecated to a future-proof value
    'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
}
default_input_feed_options = {
    'declarative_base': DefaultDeclarativeBase
}
default_expected_feed_options = {
    'declarative_base': DefaultDeclarativeBase,
    'engine_echo': False,
    'sessionmaker_kwargs': {'class_': ScrapyBulkSession},
    'orm_stmts': {default_table: _default_insert},  # Needs to be mocked
    'add': _default_add,
    'item_export_kwargs': {'add': _default_add},
    'commit': _default_commit
}


# Global Settings
class GlobalDeclarativeBase(DeclarativeBase, ScrapyDeclarativeBase):
    pass


class GlobalModel(GlobalDeclarativeBase):
    __tablename__ = 'global_table'
    id = Column(Integer, primary_key=True)


class GlobalSession(Session):
    pass


def global_orm_stmt(table):
    return 'GLOBAL ORM STMT'


def global_add(session, instance):
    pass


def global_commit(session):
    pass


global_settings_dict = deepcopy(default_settings_dict)
global_settings_dict.update({
    'SQLALCHEMY_DECLARATIVE_BASE': GlobalDeclarativeBase,
    'SQLALCHEMY_ENGINE_ECHO': True,
    'SQLALCHEMY_SESSIONMAKER_KWARGS': {'class_': GlobalSession},
    'SQLALCHEMY_DEFAULT_ORM_STMT': global_orm_stmt,
    'SQLALCHEMY_ADD': global_add,
    'SQLALCHEMY_COMMIT': global_commit,
})
global_input_feed_options = {}  # Would need 'format': 'sql' in settings.py file
global_expected_feed_options = copy(global_input_feed_options)
global_expected_feed_options.update({
    'declarative_base': GlobalDeclarativeBase,
    'engine_echo': True,
    'sessionmaker_kwargs': {'class_': GlobalSession},
    'orm_stmts': {GlobalModel.__table__: global_orm_stmt},
    'add': global_add,
    'item_export_kwargs': {'add': global_add},
    'commit': global_commit
})


# Feed Options Settings
class FeedOptionsDeclarativeBase(DeclarativeBase, ScrapyDeclarativeBase):
    pass


class FeedOptionsModel(FeedOptionsDeclarativeBase):
    __tablename__ = 'feed_options_table'
    id = Column(Integer, primary_key=True)


class FeedOptionsSession(Session):
    pass


def feed_options_orm_stmt(table):
    return 'FEED OPTIONS ORM STMT'


def feed_options_add(session, instance):
    pass


def feed_options_commit(session):
    pass


feed_options_settings_dict = deepcopy(global_settings_dict)
feed_options_input_feed_options = {
    'declarative_base': FeedOptionsDeclarativeBase,
    'engine_echo': False,
    'sessionmaker_kwargs': {'class_': FeedOptionsSession},
    'orm_stmts': {FeedOptionsModel.__table__: feed_options_orm_stmt},
    'add': feed_options_add,
    'commit': feed_options_commit
}
feed_options_expected_feed_options = copy(feed_options_input_feed_options)
feed_options_expected_feed_options['item_export_kwargs'] = {
    'add': feed_options_add
}


# Item Export Kwargs
def item_export_kwargs_add(session, instance):
    pass


item_export_kwargs_settings_dict = deepcopy(global_settings_dict)

item_export_kwargs_input_feed_options = copy(feed_options_input_feed_options)
item_export_kwargs_input_feed_options['item_export_kwargs'] = {
    'add': item_export_kwargs_add
}

item_export_kwargs_expected_feed_options = copy(
    item_export_kwargs_input_feed_options
)
item_export_kwargs_expected_feed_options['add'] = item_export_kwargs_add


