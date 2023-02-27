# here to be imported via: from scrapy_sql.utils import classproperty
import sqlalchemy
from descriptors import classproperty


def column_value_is_subquery(column_value):
    try:
        return column_value.is_clause_element
    except AttributeError:
        return False


def is_scalar_column(column):
    return isinstance(
        column.type,
        (
            sqlalchemy.sql.sqltypes.Integer,
            sqlalchemy.sql.sqltypes.Numeric
        )
    )


def insert(cls):
    return sqlalchemy.insert(cls)


def insert_ignore(cls):
    return sqlalchemy.insert(cls).prefix_with('OR IGNORE')
