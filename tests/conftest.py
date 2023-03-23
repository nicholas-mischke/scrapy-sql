
import pytest
from copy import copy, deepcopy
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.collections import InstrumentedList

# Models are the same here as in the integration_test
from integration_test.quotes.items.models import (
    QuotesBase, Author, Tag, Quote, t_quote_tag
)

kennedy_kwargs = {
    'name': 'John F. Kennedy',
    'birthday': datetime(month=5, day=29, year=1917),
    'bio': '35th president of the United States.'
}
kennedy = Author(**kennedy_kwargs)

change_kwargs = {'name': 'change'}
deep_thoughts_kwargs = {'name': 'deep-thoughts'}
change = Tag(**change_kwargs)
deep_thoughts = Tag(**deep_thoughts_kwargs)

quote_kwargs = {
    'quote': 'If not us, who? If not now, when?',
    'author': kennedy,
    'tags': [change, deep_thoughts]
}
quote = Quote(**quote_kwargs)
instances = (kennedy, change, deep_thoughts, quote)
# Instances should be provided for testing in a few seperate ways:
# 1) Empty instances that are newly initalized and in transient state
# state.transient   # !session & !identity_key
# state.pending     # session & !identity_key
# state.persistent  # session &  identity_key
# state.detached    # !session &  identity_key
# state.deleted     # session & !identity_key <-- because it was once in db


# Test DB should be SCRAPY_SQLALCHEMY_TEST as db name for MySQL & PostgreSQL.
# Test should cover SQLite, MySQL & PostgreSQL at minimum.
connection_strings = [
    # 'sqlite:///DB_TESTING.db',
    'sqlite:///memory:',
]


@pytest.fixture(params=connection_strings, scope='function')
def engine(request):
    engine = create_engine(request.param, echo=False)
    try:
        yield engine
    finally:
        engine.dispose()


session_configs = [
    {
        'class_': Session,
        'autoflush': False,
    }
]


@pytest.fixture(params=session_configs, scope='function')
def session(request, engine):
    # Just to be sure we're not working with a "dirty" db
    QuotesBase.metadata.drop_all(engine)
    QuotesBase.metadata.create_all(engine)

    Session = sessionmaker(**request.param)
    Session.configure(bind=engine)
    session = Session()

    yield session

    session.rollback()
    session.close()

    QuotesBase.metadata.drop_all(engine)


@pytest.fixture(scope='function')
def transient_instances():
    """An instance is transient when it's not in a session, or saved in db"""
    # Instead of manually resetting any changes made during testing
    # Just make new copies of the instances
    _kennedy = deepcopy(kennedy)
    _change = deepcopy(change)
    _deep_thoughts = deepcopy(deep_thoughts)

    _quote = deepcopy(quote)
    _quote.author = _kennedy
    _quote.tags = InstrumentedList([_change, _deep_thoughts])

    yield (_kennedy, _change, _deep_thoughts, _quote)


@pytest.fixture(scope='function')
def pending_session(session):
    """An instance is pending when it's in a session, but not saved in the db"""

    # Adding quote adds all instances because it's related to all others
    session.add(deepcopy(quote))

    try:
        yield session
    finally:
        session.rollback()
        session.expunge_all()  # Remove instances from session


@pytest.fixture(scope='function')
def pending_instances(pending_session):
    """An instance is pending when it's in a session, but not saved in the db"""
    # Cannot query session, since we're not using autoflush & aren't ready
    # to make the instances persistent
    new = pending_session.new

    kennedy = [
        i for i in new if
        isinstance(i, Author)
    ][0]
    change = [
        i for i in new if
        isinstance(i, Tag) and i.name == 'change'
    ][0]
    deep_thoughts = [
        i for i in new if
        isinstance(i, Tag) and i.name == 'deep-thoughts'
    ][0]
    quote = [
        i for i in new if
        isinstance(i, Quote)
    ][0]

    return kennedy, change, deep_thoughts, quote


@pytest.fixture(scope='function')
def persistent_session(session):
    """An instance is pending when it's in a session, and saved in the db"""
    session.add(deepcopy(quote))
    session.commit()

    try:
        yield session
    finally:
        session.rollback()
        session.expunge_all()  # Remove instances from session


@pytest.fixture(scope='function')
def persistent_instances(persistent_session):
    """An instance is pending when it's in a session, but not saved in the db"""

    session = persistent_session

    kennedy = session.query(Author).one()
    change = session.query(Tag).where(
        Tag.name == 'change'
    ).one()
    deep_thoughts = session.query(Tag).where(
        Tag.name == 'deep-thoughts'
    ).one()
    quote = session.query(Quote).one()

    return kennedy, change, deep_thoughts, quote


@pytest.fixture(scope='function')
def empty_quote():
    yield Quote()


@pytest.fixture(scope='function')
def transient_quote(transient_instances):
    yield transient_instances[3]


@pytest.fixture(scope='function')
def pending_quote(pending_instances):
    yield pending_instances[3]


@pytest.fixture(scope='function')
def persistent_quote(persistent_instances):
    yield persistent_instances[3]

# empty_quote, transient_quote, pending_quote, persistent_quote
