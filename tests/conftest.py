
import pytest

from datetime import datetime

from sqlalchemy import (
    create_engine,
    Column, DateTime, ForeignKey, Integer, String, Table, Text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm.collections import InstrumentedList

from scrapy_sql import ScrapyDeclarativeMetaAdapter


@pytest.fixture()
def connection():
    pass


@pytest.fixture()
def create_tables():
    pass


@pytest.fixture()
def seeded_db():
    pass

@pytest.fixture()
def session():
    pass



@pytest.fixture()
def create_tables(conn_str=':sqlite:///memory:'):
    engine = create_engine(conn_str)
    Base = declarative_base()

    class Author(Base, ScrapyDeclarativeMetaAdapter):
        __tablename__ = 'author'

        id = Column(Integer, primary_key=True)

        name = Column(String(50), unique=True)
        birthday = Column(DateTime)
        bio = Column(Text)

    class Quote(Base, ScrapyDeclarativeMetaAdapter):
        __tablename__ = 'quote'

        id = Column(Integer, primary_key=True)
        author_id = Column(ForeignKey('author.id'))

        quote = Column(Text)

        author = relationship('Author')
        tags = relationship('Tag', secondary='quote_tag')

    class Tag(Base, ScrapyDeclarativeMetaAdapter):
        __tablename__ = 'tag'

        id = Column(Integer, primary_key=True)
        name = Column(String(31), unique=True)

    t_quote_tag = Table(
        'quote_tag', Base.metadata,
        Column('quote_id', ForeignKey('quote.id')),
        Column('tag_id', ForeignKey('tag.id'))
    )

    # Create tables if not exists
    Base.metadata.create_all(engine)

    yield {
        'engine': engine,
        'author': Author,
        'quote': Quote,
        'Tag': Tag,
        't_quote_tag': t_quote_tag
    }


@pytest.fixture()
def insert_data(create_tables):

    Session = sessionmaker()
    Session.configure(bind=create_tables.get('engine'))
    session = Session()

    einstein = create_tables.get('author')()
    kennedy = create_tables.get('author')()

    change = create_tables.get('tag')()
    deep_thoughts = create_tables.get('tag')()
    inspirational = create_tables.get('tag')()
    community = create_tables.get('tag')()

    einstein_quote_I = create_tables.get('quote')()
    einstein_quote_II = create_tables.get('quote')()

    kennedy_quote_I = create_tables.get('quote')()
    kennedy_quote_II = create_tables.get('quote')()

    einstein.name, einstein.birthday, einstein.bio = (
        'Albert Einstein',
        datetime(month=3, day=14, year=1879),
        'Won the 1921 Nobel Prize in Physics.'
    )
    session.add(einstein)
    session.commit()

    kennedy.name, kennedy.birthday, kennedy.bio = (
        'John F. Kennedy',
        datetime(month=5, day=29, year=1917),
        '35th president of the United States.'
    )
    session.add(kennedy)
    session.commit()

    change.name = 'change'
    deep_thoughts.name = 'deep_thoughts'
    inspirational.name = 'inspirational'
    community.name = 'community'

    session.add(change)
    session.commit()
    session.add(deep_thoughts)
    session.commit()
    session.add(inspirational)
    session.commit()
    session.add(community)
    session.commit()

    einstein_quote_I.quote, einstein_quote_I.author, einstein_quote_I.tags = (
        (
            'The world as we have created it is a process of our thinking. '
            'It cannot be changed without changing our thinking.'
        ),
        einstein,
        InstrumentedList([change, deep_thoughts])
    )
    einstein_quote_II.quote, einstein_quote_II.author, einstein_quote_II.tags = (
        (
            'There are only two ways to live your life. '
            'One is as though nothing is a miracle. '
            'The other is as though everything is a miracle.'
        ),
        einstein,
        InstrumentedList(inspirational)
    )
    session.add(einstein_quote_I)
    session.commit()
    session.add(einstein_quote_II)
    session.commit()

    kennedy_quote_I.quote, kennedy_quote_I.author, kennedy_quote_I.tags = (
        'If not us, who? If not now, when?',
        kennedy,
        InstrumentedList([change, deep_thoughts])
    )
    kennedy_quote_II.quote, kennedy_quote_II.author, kennedy_quote_II.tags = (
        (
            'Ask not what your country can do for you, '
            'but what you can do for your country.'
        ),
        community,
        InstrumentedList(inspirational)
    )
    session.add(kennedy_quote_I)
    session.commit()
    session.add(kennedy_quote_II)
    session.commit()

    yield {
        'session': session
    }
