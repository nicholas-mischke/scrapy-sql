
import sqlalchemy
from descriptors import classproperty # here to be imported with scrapy_sql.utils import classproperty


def column_value_is_subquery(column_value):
    try:
        return column_value.is_clause_element
    except AttributeError:  # is str
        return False


def insert(cls):
    return sqlalchemy.insert(cls)


def insert_ignore(cls):
    return sqlalchemy.insert(cls).prefix_with('OR IGNORE')
