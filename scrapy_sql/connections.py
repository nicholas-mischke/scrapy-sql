from pathlib import Path
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
# from scrapy.utils.spider import iter_spider_classes
from scrapy.utils.project import get_project_settings
# from sqlalchemy.pool import NullPool

# Allow connection to multiple databases
# https://docs.sqlalchemy.org/en/14/orm/persistence_techniques.html#session-partitioning
connection_info, base_engine_mapping = {}, {}
project_settings = get_project_settings()


def iter_conn_strs():
    FEEDS = {}
    FEEDS.update(project_settings.getdict('FEEDS'))

    # A later update should scan the project for spider classes
    # Then add all FEEDS in the spider.custom_settings cls attr.

    return tuple(
        uri for uri, option_dict in FEEDS.items()
        if option_dict.get('format') == 'sql'
    )


def get_database_name(engine):
    conn_args, conn_kwargs = engine.dialect.create_connect_args(engine.url)
    database_name = conn_kwargs.get('db')
    if database_name:  # non SQLite
        return database_name
    return Path(conn_args[0]).stem


for conn_str in iter_conn_strs():
    # https://stackoverflow.com/questions/21738944/how-to-close-a-sqlalchemy-session

    # session.close() will give the connection back to the connection pool
    # of Engine and doesn't close the connection.
    # engine.dispose() will close all connections of the connection pool.
    # Engine will not use connection pool if you set poolclass = NullPool.
    # So the connection(SQLAlchemy session) will close directly
    # after session.close().

    # echo=True assists in debugging.
    engine = create_engine(
        conn_str,
        echo=project_settings.getbool('SQLALCHEMY_ENGINE_ECHO'),
        # poolclass=NullPool
    )
    database = get_database_name(engine)
    Base = declarative_base()

    base_engine_mapping[Base] = engine
    connection_info[database] = {
        'conn_str': conn_str,
        'engine':   engine,
        'Base':     Base,
    }
