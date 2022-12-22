
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from pathlib import Path
from scrapy.utils.project import get_project_settings


def get_database_name(engine):
    conn_args, conn_kwargs = engine.dialect.create_connect_args(engine.url)
    database_name = conn_kwargs.get('db')
    if database_name:  # non SQLite
        return database_name
    return Path(conn_args[0]).stem


def get_sqlalchemy_connection_strings():
    settings = get_project_settings()
    return [
        uri for uri, option_dict in settings.getdict('FEEDS').items()
        if option_dict.get('format') == 'sql'
    ]


databases_info = {}

# Allow connection to multiple databases
# https://docs.sqlalchemy.org/en/14/orm/persistence_techniques.html#session-partitioning
base_engine_mapping = {}

for conn_str in get_sqlalchemy_connection_strings():

    engine = create_engine(conn_str, echo=False)
    database = get_database_name(engine)
    Base = declarative_base()
    metadata = Base.metadata

    databases_info[database] = {
        'conn_str': conn_str,
        'engine':   engine,
        'Base':     Base,
        'metadata': metadata
    }

    base_engine_mapping[Base] = engine


def create_new_session():
    Session = sessionmaker()
    Session.configure(binds=base_engine_mapping)
    return Session()


session = create_new_session()
