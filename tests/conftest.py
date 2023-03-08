
import pytest

from copy import deepcopy
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Models are the same here as in the integration_test
from integration_test.quotes.items.models import (
    QuotesBase, Author, Tag, Quote, t_quote_tag
)


# Empty Transient Instance Fixtures
@pytest.fixture(scope='function')
def empty_Author():
    yield Author()


@pytest.fixture(scope='function')
def empty_Tag():
    yield Tag()


@pytest.fixture(scope='function')
def empty_Quote():
    yield Quote()


################################################################################
######### ▼ ▼ ▼  Declare Kwargs & Initialize Transient Instances  ▼ ▼ ▼ ########
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
deep_thoughts_kwargs = {'name': 'deep-thoughts'}
inspirational_kwargs = {'name': 'inspirational'}
community_kwargs = {'name': 'community'}

# Tag Instances
change_instance = Tag(**change_kwargs)
deep_thoughts_instance = Tag(**deep_thoughts_kwargs)
inspirational_instance = Tag(**inspirational_kwargs)
community_instance = Tag(**community_kwargs)

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

# Test DB should be SCRAPY_SQLALCHEMY_TEST as db name for MySQL, PostgreSQL
@pytest.fixture(params=[':sqlite:///memory:'], scope='session')
def db_engine(request):
    engine = create_engine(request.param)
    try:
        yield engine
    finally:
        engine.dispose()

@pytest.fixture(scope='session')
def transient_instances():
    yield {
        'einstein': deepcopy(einstein_instance),
        'kennedy': deepcopy(kennedy_instance),

        'change': deepcopy(change_instance),
        'deep_thoughts': deepcopy(deep_thoughts_instance),
        'inspirational': deepcopy(inspirational_instance),
        'community': deepcopy(community_instance),

        'einstein_quote_I': deepcopy(einstein_quote_I_instance),
        'einstein_quote_II': deepcopy(einstein_quote_II_instance),
        'kennedy_quote_I': deepcopy(kennedy_quote_I_instance),
        'kennedy_quote_II': deepcopy(kennedy_quote_II_instance),
    }

@pytest.fixture(scope='session')
def pending_instances(db_engine, transient_instances):
    Session = sessionmaker()
    Session.configure(bind=db_engine)
    session = Session()


@pytest.fixture(scope='session')
def persistent_instances():
    Session = sessionmaker()
    session = Session()





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
