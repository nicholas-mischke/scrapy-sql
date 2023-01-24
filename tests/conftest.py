
from datetime import datetime

import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data.models import *
from data.rows import *


@pytest.fixture(params=[':sqlite:///memory:'], scope='session')
def db_engine(request):
    return create_engine(request.param)


@pytest.fixture(scope='session')
def empty_db(db_engine):
    Base.metadata.create_all(db_engine)

    yield

    Base.metadata.drop_all()


@pytest.fixture(scope='session')
def seeded_db(db_engine, empty_db):

    Session = sessionmaker()
    Session.configure(bind=db_engine)

    with Session() as session:
        session.add_all(
            [
                einstein_quote_I, einstein_quote_II,
                kennedy_quote_I, kennedy_quote_II
            ]
        )
        session.commit()

    yield


@pytest.fixture
def session(db_engine, empty_db):
    Session = sessionmaker()
    Session.configure(bind=db_engine)

    with Session.begin() as session:
        yield session


@pytest.fixture
def scrapy_session(db_engine, empty_db):
    return


@pytest.fixture
def seeded_session():
    pass
