"""
Essential SQLAlchemy, 2nd Edition - Jason Myers & Rick Copeland
page 105 - 106

Transient
    The instance is not in session, and is not in the database.
Pending
    The instance has been added to the session with add(), but hasn't
    been flushed or committed.
Persistent
    The object in session has a corresponding record in the database.
Detached
    The instance is no longer connected to the session, but has a record in
    the database.
"""

import pytest
from copy import deepcopy

from .models import Author, Tag, Quote

from .instance_kwargs import (
    # Authors
    einstein_instance, kennedy_instance,

    # Tags
    change_instance, community_instance,
    deep_thoughts_instance, inspirational_instance,

    # Quotes
    einstein_quote_I_instance, einstein_quote_II_instance,
    kennedy_quote_I_instance, kennedy_quote_II_instance
)


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
def einstein():
    yield deepcopy(einstein_instance)


@pytest.fixture(scope='session')
def kennedy():
    yield deepcopy(kennedy_instance)

# Tags


@pytest.fixture(scope='session')
def change():
    yield deepcopy(change_instance)


@pytest.fixture(scope='session')
def community():
    yield deepcopy(community_instance)


@pytest.fixture(scope='session')
def deep_thoughts():
    yield deepcopy(deep_thoughts_instance)


@pytest.fixture(scope='session')
def inspirational():
    yield deepcopy(inspirational_instance)

# Quotes


@pytest.fixture(scope='session')
def einstein_quote_I():
    yield deepcopy(einstein_quote_I_instance)


@pytest.fixture(scope='session')
def einstein_quote_II():
    yield deepcopy(einstein_quote_II_instance)


@pytest.fixture(scope='session')
def kennedy_quote_I():
    yield deepcopy(kennedy_quote_I_instance)


@pytest.fixture(scope='session')
def kennedy_quote_II():
    yield deepcopy(kennedy_quote_II_instance)
