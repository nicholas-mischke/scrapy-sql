
from datetime import datetime

import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data.models import *


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
    session = Session()

    change = Tag(**{'name': 'change'})
    deep_thoughts = Tag({'name': 'deep-thoughts'})
    inspirational = Tag({'name': 'inspirational'})
    community = Tag({'name': 'community'})

    einstein = Author(**{
        'name': 'Albert Einstein',
        'birthday': datetime(month=3, day=14, year=1879),
        'bio': 'Won the 1921 Nobel Prize in Physics.'
    })
    kennedy = Author(**{
        'name': 'John F. Kennedy',
        'birthday': datetime(month=5, day=29, year=1917),
        'bio': '35th president of the United States.'
    })

    einstein_quote_I = Quote(**{
        'quote': (
            'The world as we have created it is a process of our thinking. '
            'It cannot be changed without changing our thinking.'
        ),
        'author': einstein,
        'tags': [change, deep_thoughts]
    })
    einstein_quote_II = Quote(**{
        'quote': (
            'There are only two ways to live your life. '
            'One is as though nothing is a miracle. '
            'The other is as though everything is a miracle.'
        ),
        'author': einstein,
        'tags': [inspirational]
    })

    kennedy_quote_I = Quote(**{
        'quote': 'If not us, who? If not now, when?',
        'author': kennedy,
        'tags': [change, deep_thoughts]
    })
    kennedy_quote_II = Quote(**{
        'quote': (
            'Ask not what your country can do for you, '
            'but what you can do for your country.'
        ),
        'author': kennedy,
        'tags': [community]
    })

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

    session = Session()

    yield session

    session.rollback()


@pytest.fixture
def seeded_session():
    pass
