
import pytest
from datetime import datetime
from pathlib import Path

from sqlalchemy import (
    create_engine,
    Column, DateTime, ForeignKey,
    Integer, String, Table, Text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.pool import NullPool

from scrapy_sql import ScrapyDeclarativeMetaAdapter, SQLAlchemyTableAdapter


# @pytest.fixture(scope='session')
# def generate_sqlalchemy_objects(conn_str):
#     # Setup
#     engine = create_engine(conn_str)  # , poolclass=NullPool)
#     Base = declarative_base()
#     metadata = Base.metadata

#     Session = sessionmaker()
#     Session.configure(bind=engine)
#     session = Session()

#     return engine, Base, metadata, session


# @pytest.fixture(scope='function')
# def generate_session(engine):
#     Session = sessionmaker()
#     Session.configure(bind=engine)
#     session = Session()

#     return session


# @pytest.fixture(scope='session')
# def create_tables(engine, Base, metadata):

#     class Author(Base, ScrapyDeclarativeMetaAdapter):
#         __tablename__ = 'author'

#         id = Column(Integer, primary_key=True)

#         name = Column(String(50), unique=True)
#         birthday = Column(DateTime)
#         bio = Column(Text)

#     class Quote(Base, ScrapyDeclarativeMetaAdapter):
#         __tablename__ = 'quote'

#         id = Column(Integer, primary_key=True)
#         author_id = Column(ForeignKey('author.id'))

#         quote = Column(Text)

#         # Relationships
#         author = relationship('Author')
#         tags = relationship('Tag', secondary='quote_tag')

#     class Tag(Base, ScrapyDeclarativeMetaAdapter):
#         __tablename__ = 'tag'

#         id = Column(Integer, primary_key=True)
#         name = Column(String(31), unique=True)

#     t_quote_tag = Table(
#         'quote_tag', metadata,
#         Column('quote_id', ForeignKey('quote.id')),
#         Column('tag_id', ForeignKey('tag.id'))
#     )

#     # Create tables if not exists
#     metadata.create_all(engine)

#     return Author, Quote, Tag, t_quote_tag


# @pytest.fixture(scope='session')
# def insert_data(session):

#     Author, Quote, Tag, t_quote_tag =

#     einstein = Author()
#     einstein.name, einstein.birthday, einstein.bio = (
#         'Albert Einstein',
#         datetime(year=1879, month=3, day=14),
#         'Won the 1921 Nobel Prize in Physics for his paper explaining the photoelectric effect.'
#     )
#     session.add(einstein)
#     session.commit()

#     kennedy = Author()
#     kennedy.name, kennedy.birthday, kennedy.bio = (
#         'John F. Kennedy',
#         datetime(year=1917, month=5, day=29),
#         'American politician who served as the 35th president of the United States.'
#     )
#     session.add(kennedy)
#     session.commit()

#     # Generate Tags
#     change = Tag()
#     change.name = 'change'
#     session.add(change)
#     session.commit()

#     deep_thoughts = Tag()
#     deep_thoughts.name = 'deep_thoughts'
#     session.add(deep_thoughts)
#     session.commit()

#     inspirational = Tag()
#     inspirational.name = 'inspirational'
#     session.add(inspirational)
#     session.commit()

#     community = Tag()
#     community.name = 'community'
#     session.add(community)
#     session.commit()

#     # Generate Quotes
#     einstein_quote_I = Quote()
#     einstein_quote_I.quote = 'The world as we have created it is a process of our thinking. It cannot be changed without changing our thinking.'
#     einstein_quote_I.author = einstein
#     einstein_quote_I.tags.append(change)
#     einstein_quote_I.tags.append(deep_thoughts)
#     session.add(einstein_quote_I)
#     session.commit()

#     kennedy_quote_I = Quote()
#     kennedy_quote_I.quote = 'If not us, who? If not now, when?'
#     kennedy_quote_I.author = kennedy
#     kennedy_quote_I.tags.append(change)
#     kennedy_quote_I.tags.append(deep_thoughts)
#     session.add(kennedy_quote_I)
#     session.commit()

#     einstein_quote_II = Quote()
#     einstein_quote_II.quote = 'There are only two ways to live your life. One is as though nothing is a miracle. The other is as though everything is a miracle.'
#     einstein_quote_II.author = einstein
#     einstein_quote_II.tags.append(inspirational)
#     session.add(einstein_quote_II)
#     session.commit()

#     kennedy_quote_II = Quote()
#     kennedy_quote_II.quote = 'Ask not what your country can do for you, but what you can do for your country.'
#     kennedy_quote_II.author = kennedy
#     kennedy_quote_II.tags.append(community)
#     session.add(kennedy_quote_II)
#     session.commit()


# def insert():

# def create_tables():

# def setup_db():
#     # Setup
#     engine = create_engine(conn_str)  # , poolclass=NullPool)
#     Base = declarative_base()
#     metadata = Base.metadata

#     Session = sessionmaker()
#     Session.configure(bind=engine)
#     session = Session()

#     return engine, Base, metadata, session


def build_db(conn_str):

    # Setup
    engine = create_engine(conn_str)  # , poolclass=NullPool)
    Base = declarative_base()
    metadata = Base.metadata

    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()

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

        # Relationships
        author = relationship('Author')
        tags = relationship('Tag', secondary='quote_tag')

    class Tag(Base, ScrapyDeclarativeMetaAdapter):
        __tablename__ = 'tag'

        id = Column(Integer, primary_key=True)
        name = Column(String(31), unique=True)

    t_quote_tag = Table(
        'quote_tag', metadata,
        Column('quote_id', ForeignKey('quote.id')),
        Column('tag_id', ForeignKey('tag.id'))
    )

    # Create tables if not exists
    metadata.create_all(engine)

    # Tables are added and commited to the table in a manner that allows
    # The id columns for each table to be consentant

    # Generate Authors
    einstein = Author()
    einstein.name, einstein.birthday, einstein.bio = (
        'Albert Einstein',
        datetime(year=1879, month=3, day=14),
        'Won the 1921 Nobel Prize in Physics for his paper explaining the photoelectric effect.'
    )
    session.add(einstein)
    session.commit()

    kennedy = Author()
    kennedy.name, kennedy.birthday, kennedy.bio = (
        'John F. Kennedy',
        datetime(year=1917, month=5, day=29),
        'American politician who served as the 35th president of the United States.'
    )
    session.add(kennedy)
    session.commit()

    # Generate Tags
    change = Tag()
    change.name = 'change'
    session.add(change)
    session.commit()

    deep_thoughts = Tag()
    deep_thoughts.name = 'deep_thoughts'
    session.add(deep_thoughts)
    session.commit()

    inspirational = Tag()
    inspirational.name = 'inspirational'
    session.add(inspirational)
    session.commit()

    community = Tag()
    community.name = 'community'
    session.add(community)
    session.commit()

    # Generate Quotes
    einstein_quote_I = Quote()
    einstein_quote_I.quote = 'The world as we have created it is a process of our thinking. It cannot be changed without changing our thinking.'
    einstein_quote_I.author = einstein
    einstein_quote_I.tags.append(change)
    einstein_quote_I.tags.append(deep_thoughts)
    session.add(einstein_quote_I)
    session.commit()

    kennedy_quote_I = Quote()
    kennedy_quote_I.quote = 'If not us, who? If not now, when?'
    kennedy_quote_I.author = kennedy
    kennedy_quote_I.tags.append(change)
    kennedy_quote_I.tags.append(deep_thoughts)
    session.add(kennedy_quote_I)
    session.commit()

    einstein_quote_II = Quote()
    einstein_quote_II.quote = 'There are only two ways to live your life. One is as though nothing is a miracle. The other is as though everything is a miracle.'
    einstein_quote_II.author = einstein
    einstein_quote_II.tags.append(inspirational)
    session.add(einstein_quote_II)
    session.commit()

    kennedy_quote_II = Quote()
    kennedy_quote_II.quote = 'Ask not what your country can do for you, but what you can do for your country.'
    kennedy_quote_II.author = kennedy
    kennedy_quote_II.tags.append(community)
    session.add(kennedy_quote_II)
    session.commit()

    return {
        'session': session,
        'Author': Author,
        'Quote': Quote,
        'Tag': Tag,
        't_quote_tag': t_quote_tag
    }

    # Teardown
    # session.close()


@pytest.fixture(scope='session')
def sqlite_db():
    return build_db('sqlite:///:memory:')


@pytest.fixture
def mysql_db():
    pass


@pytest.fixture
def postgresql_db():
    pass


@pytest.fixture(scope='session')
def db_tables(sqlite_db):
    return [
        sqlite_db.get('Author'),
        sqlite_db.get('Quote'),
        sqlite_db.get('Tag'),
        sqlite_db.get('t_quote_tag')
    ]


@pytest.fixture(scope='session')
def author_table(db_tables):
    return db_tables[0]


@pytest.fixture(scope='session')
def einstein(sqlite_db):
    """
    Used as a generic table obj to run test on
    """
    return sqlite_db.get('session').query(
        sqlite_db.get('Author')
    ).filter_by(
        **{'name': 'Albert Einstein'}
    ).first()


@pytest.fixture(scope='session')
def einstein_tableadapter(einstein):
    return SQLAlchemyTableAdapter(einstein)


if __name__ == '__main__':
    pass
