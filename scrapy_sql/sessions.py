
# Scrapy imports
from scrapy.utils.project import get_project_settings

# SQLAlchemy imports
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Project imports

# 3rd party imports
from pathlib import Path

def get_sqlalchemy_connection_strings():
    settings = get_project_settings()

    # settings = scrapy.settings.Settings()
    # settings.setmodule(module='tests.data.settings', priority='project')

    return [
        uri for uri, option_dict in settings.getdict('FEEDS').items()
        if option_dict.get('format') == 'sql'
    ]


def get_database_name(engine):
    conn_args, conn_kwargs = engine.dialect.create_connect_args(engine.url)
    database_name = conn_kwargs.get('db')
    if database_name:  # non SQLite
        return database_name
    return Path(conn_args[0]).stem


def generate_connection_data(conn_strs=get_sqlalchemy_connection_strings()):
    databases_info = {}

    # Allow connection to multiple databases
    # https://docs.sqlalchemy.org/en/14/orm/persistence_techniques.html#session-partitioning
    base_engine_mapping = {}

    for conn_str in conn_strs:
        # https://stackoverflow.com/questions/21738944/how-to-close-a-sqlalchemy-session

        # session.close() will give the connection back to the connection pool
        # of Engine and doesn't close the connection.
        # engine.dispose() will close all connections of the connection pool.
        # Engine will not use connection pool if you set poolclass = NullPool.
        # So the connection(SQLAlchemy session) will close directly
        # after session.close().

        # echo=True assists in debugging.
        engine = create_engine(conn_str, echo=False)  # poolclass=NullPool
        database = get_database_name(engine)
        Base = declarative_base()
        metadata = Base.metadata

        base_engine_mapping[Base] = engine
        databases_info[database] = {
            'conn_str': conn_str,
            'engine':   engine,
            'Base':     Base,
            'metadata': metadata
        }

    return databases_info, base_engine_mapping


databases_info, base_engine_mapping = generate_connection_data()


def create_new_session(base_engine_mapping=base_engine_mapping):
    Session = sessionmaker()
    Session.configure(binds=base_engine_mapping)
    return Session()

session = None