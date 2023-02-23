
from sqlalchemy import insert


def column_value_is_subquery(column_value):
    try:
        return column_value.is_clause_element
    except AttributeError:  # is str
        return False


def insert(cls):
    return insert(cls)


def insert_ignore(cls):
    return insert(cls).prefix_with('OR IGNORE')
