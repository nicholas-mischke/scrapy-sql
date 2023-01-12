
import pytest
from unittest.mock import patch, MagicMock

from scrapy_sql.sessions import *
import scrapy_sql
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base

# Mock scrapy.utils.project.get_project_settings function here
import scrapy


# @pytest.fixture(scope='module')
def get_project_settings_test_double():
    settings = scrapy.settings.Settings()
    settings.setmodule(module='tests.data.settings', priority='project')
    return settings


@pytest.fixture()  # scope='module')
def test_engine(tmp_path):
    sqlite_path = tmp_path / 'silly_test_db.db'
    conn_str = f"sqlite:///{sqlite_path}"
    engine = create_engine(conn_str)
    yield engine
    engine.dispose()


@patch('scrapy_sql.sessions.get_project_settings', get_project_settings_test_double)
def test_get_sqlalchemy_connection_strings():
    # These settings are defined in data/settings.py
    assert get_sqlalchemy_connection_strings() == [
        'sqlite:///quotes.db'
    ]


def test_get_database_name(test_engine):
    assert get_database_name(test_engine) == 'silly_test_db'


@patch('scrapy_sql.sessions.get_project_settings', get_project_settings_test_double)
def test_generate_connection_data():

    databases_info, base_engine_mapping = generate_connection_data()

    for name, info in databases_info.items():
        conn_str, engine, base, metadata = tuple(info.values())

        assert name in conn_str and isinstance(conn_str, str)
        assert (
            engine.url == conn_str
            and isinstance(engine, sqlalchemy.engine.base.Engine)
        )
        assert isinstance(base, sqlalchemy.orm.decl_api.Base)
        assert (
            base.metadata == metadata
            and isinstance(metadata, sqlalchemy.sql.schema.MetaData)
        )

    for base, engine in base_engine_mapping.items():
        assert isinstance(base, sqlalchemy.orm.decl_api.Base)
        assert isinstance(engine, sqlalchemy.engine.base.Engine)


def test_create_new_session():
    assert isinstance(create_new_session(), sqlalchemy.orm.session.Session)
