
from copy import deepcopy
from datetime import datetime

import pytest
from sqlalchemy import (Column, Date, ForeignKey, Integer, String, Table, Text,
                        create_engine)
from sqlalchemy.orm import DeclarativeBase, relationship, sessionmaker

from scrapy_sql import ScrapyDeclarativeBaseExtension


################################################################################
######################### ▼ ▼ ▼  Define Models  ▼ ▼ ▼ ##########################
################################################################################
class QuotesBase(DeclarativeBase):
    pass


class Author(QuotesBase, ScrapyDeclarativeBaseExtension):
    __tablename__ = 'author'

    id = Column(Integer, primary_key=True)

    name = Column(String(50), unique=True, nullable=False)
    birthday = Column(Date, nullable=False)
    bio = Column(Text, nullable=False)


class Tag(QuotesBase, ScrapyDeclarativeBaseExtension):
    __tablename__ = 'tag'

    id = Column(Integer, primary_key=True)
    name = Column(String(31), unique=True, nullable=False)


class Quote(QuotesBase, ScrapyDeclarativeBaseExtension):
    __tablename__ = 'quote'

    id = Column(Integer, primary_key=True)
    author_id = Column(ForeignKey('author.id'), nullable=False)

    quote = Column(Text, nullable=False, unique=True)

    author = relationship('Author')
    tags = relationship('Tag', secondary='quote_tag')


t_quote_tag = Table(
    'quote_tag', QuotesBase.metadata,
    Column('quote_id', ForeignKey('quote.id'), primary_key=True),
    Column('tag_id',   ForeignKey('tag.id'),   primary_key=True)
)
################################################################################
######################### ▲ ▲ ▲  Define Models  ▲ ▲ ▲ ##########################
################################################################################


################################################################################
################### ▼ ▼ ▼  Declare Kwargs & Instances  ▼ ▼ ▼ ###################
################################################################################

# Author Kwargs
einstein_kwargs = {
    'name': 'Albert Einstein',
    'birthday': datetime(month=3, day=14, year=1879),
    'bio': 'Won the 1921 Nobel Prize in Physics.'
}
kennedy_kwargs = {
    'name': 'John F. Kennedy',
    'birthday': datetime(month=5, day=29, year=1917),
    'bio': '35th president of the United States.'
}
# Author Instances
einstein_instance = Author(**einstein_kwargs)
kennedy_instance = Author(**kennedy_kwargs)

# Tag Kwargs
change_kwargs = {'name': 'change'}
community_kwargs = {'name': 'community'}
deep_thoughts_kwargs = {'name': 'deep-thoughts'}
inspirational_kwargs = {'name': 'inspirational'}
# Tag Instances
change_instance = Tag(**change_kwargs)
community_instance = Tag(**community_kwargs)
deep_thoughts_instance = Tag(**deep_thoughts_kwargs)
inspirational_instance = Tag(**inspirational_kwargs)

# Quote Kwargs
einstein_quote_I_kwargs = {
    'quote': (
        'The world as we have created it is a process of our thinking. '
        'It cannot be changed without changing our thinking.'
    ),
    'author': einstein_instance,
    'tags': [change_instance, deep_thoughts_instance]
}
einstein_quote_II_kwargs = {
    'quote': (
        'There are only two ways to live your life. '
        'One is as though nothing is a miracle. '
        'The other is as though everything is a miracle.'
    ),
    'author': einstein_instance,
    'tags': [inspirational_instance]
}
kennedy_quote_I_kwargs = {
    'quote': 'If not us, who? If not now, when?',
    'author': kennedy_instance,
    'tags': [change_instance, deep_thoughts_instance]
}
kennedy_quote_II_kwargs = {
    'quote': (
        'Ask not what your country can do for you, '
        'but what you can do for your country.'
    ),
    'author': kennedy_instance,
    'tags': [community_instance]
}
# Quote Instances
einstein_quote_I_instance = Quote(**einstein_quote_I_kwargs)
einstein_quote_II_instance = Quote(**einstein_quote_II_kwargs)
kennedy_quote_I_instance = Quote(**kennedy_quote_I_kwargs)
kennedy_quote_II_instance = Quote(**kennedy_quote_II_kwargs)
################################################################################
################### ▲ ▲ ▲  Declare Kwargs & Instances  ▲ ▲ ▲ ###################
################################################################################


################################################################################
##### ▼ ▼ ▼  Fixtures for Transient, Pending & Persistent Instances ▼ ▼ ▼ ######
################################################################################


# Empty Instances


@pytest.fixture(scope='function')
def empty_Author():
    yield Author()


@pytest.fixture(scope='function')
def empty_Tag():
    yield Tag()


@pytest.fixture(scope='function')
def empty_Quote():
    yield Quote()


# Authors


@pytest.fixture(scope='session')
def transient_einstein():
    yield deepcopy(einstein_instance)


@pytest.fixture(scope='session')
def transient_kennedy():
    yield deepcopy(kennedy_instance)

# Tags


@pytest.fixture(scope='session')
def transient_change():
    yield deepcopy(change_instance)


@pytest.fixture(scope='session')
def transient_community():
    yield deepcopy(community_instance)


@pytest.fixture(scope='session')
def transient_deep_thoughts():
    yield deepcopy(deep_thoughts_instance)


@pytest.fixture(scope='session')
def transient_inspirational():
    yield deepcopy(inspirational_instance)


# Quotes

@pytest.fixture(scope='session')
def transient_einstein_quote_I():
    yield deepcopy(einstein_quote_I_instance)


@pytest.fixture(scope='session')
def transient_einstein_quote_II():
    yield deepcopy(einstein_quote_II_instance)


@pytest.fixture(scope='session')
def transient_kennedy_quote_I():
    yield deepcopy(kennedy_quote_I_instance)


@pytest.fixture(scope='session')
def transient_kennedy_quote_II():
    yield deepcopy(kennedy_quote_II_instance)


################################################################################
##### ▲ ▲ ▲  Fixtures for Transient, Pending & Persistent Instances ▲ ▲ ▲ ######
################################################################################

# Test DB should be SCRAPY_SQLALCHEMY_TEST as db name for MySQL, PostgreSQL
@pytest.fixture(params=[':sqlite:///memory:'], scope='session')
def db_engine(request):
    engine = create_engine(request.param)

    try:
        yield engine
    finally:
        engine.close()
        engine.dispose()


# # # # # @pytest.fixture(scope='session')
# # # # # def session(db_engine):
# # # # #     Session = sessionmaker()
# # # # #     Session.configure(bind=db_engine)
# # # # #     session = Session()

# # # # #     yield session

# # # # #     session.rollback()


# # # # # @pytest.fixture(scope='session')
# # # # # def empty_db(db_engine, session):
# # # # #     Base.metadata.drop_all()
# # # # #     Base.metadata.create_all(db_engine)

# # # # #     yield session

# # # # #     Base.metadata.drop_all()


# # # # # @pytest.fixture(scope='session')
# # # # # def seeded_session(
# # # # #     empty_db,
# # # # #     transient_quotes=(
# # # # #         transient.einstein_quote_I,
# # # # #         transient.einstein_quote_II,
# # # # #         transient.kennedy_quote_I,
# # # # #         transient.kennedy_quote_II
# # # # #     )
# # # # # ):
# # # # #     # state = pending
# # # # #     empty_db.add_all(transient_quotes)
# # # # #     yield empty_db


# # # # # @pytest.fixture(scope='session')
# # # # # def seeded_db(seeded_session):
# # # # #     # state = persistent
# # # # #     seeded_session.commit()
# # # # #     yield seeded_session
