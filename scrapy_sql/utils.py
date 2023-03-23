
import sqlalchemy

# here to be imported via: from scrapy_sql.utils import classproperty
# used in models.py to add a stmt property to a DeclarativeBase subclass
from descriptors import classproperty
import re


def clean_text(text):

    text = str(text).encode('ascii', 'ignore').decode('utf-8')

    # Turn multiple whitespaces into single whitespace
    _RE_COMBINE_WHITESPACE = re.compile(r"(?a:\s+)")
    _RE_STRIP_WHITESPACE = re.compile(r"(?a:^\s+|\s+$)")

    text = _RE_COMBINE_WHITESPACE.sub(" ", text)
    text = _RE_STRIP_WHITESPACE.sub("", text)

    return text


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

# TODO string builder
def subquery_to_string(subquery):
    string_subquery = clean_text(subquery)
    for key, value in subquery.compile().params.items():
        if isinstance(value, str):
            value = f'"{value}"'
        string_subquery = string_subquery.replace(f':{key}', str(value))
    return string_subquery.lstrip('(').rstrip(')')


def insert(cls):
    return sqlalchemy.insert(cls)

# TODO adjust insert_ignore on a driver by driver basis
def insert_ignore(cls):
    return sqlalchemy.insert(cls).prefix_with('OR IGNORE')
