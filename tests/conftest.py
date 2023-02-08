
import pytest

# from scrapy_sql.session import ScrapySession
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data.models import Base
from data import transient


# Test DB should be SCRAPY_SQLALCHEMY_TEST as db name
@pytest.fixture(params=[':sqlite:///memory:'], scope='session')
def db_engine(request):
    engine = create_engine(request.param)

    yield engine

    engine.close()
    engine.dispose()


@pytest.fixture(scope='session')
def session(db_engine):
    Session = sessionmaker()
    Session.configure(bind=db_engine)
    session = Session()

    yield session

    session.rollback()


@pytest.fixture(scope='session')
def empty_db(db_engine, session):
    Base.metadata.drop_all()
    Base.metadata.create_all(db_engine)

    yield session

    Base.metadata.drop_all()


@pytest.fixture(scope='session')
def seeded_session(
    empty_db,
    transient_quotes=(
        transient.einstein_quote_I,
        transient.einstein_quote_II,
        transient.kennedy_quote_I,
        transient.kennedy_quote_II
    )
):
    # state = pending
    empty_db.add_all(transient_quotes)
    yield empty_db


@pytest.fixture(scope='session')
def seeded_db(seeded_session):
    # state = persistent
    seeded_session.commit()
    yield seeded_session
