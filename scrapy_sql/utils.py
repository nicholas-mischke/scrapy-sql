
# Scrapy / Twisted Imports
from scrapy.utils.misc import load_object

# SQLAlchemy Imports
import sqlalchemy
from sqlalchemy import Table
from sqlalchemy.orm.decl_api import DeclarativeAttributeIntercept

# 3rd 🎉 Imports
from inspect import isclass, isfunction
import re
# here to be imported via: from scrapy_sql.utils import classproperty
# used in models.py to add a stmt property to a DeclarativeBase subclass
#from descriptors import classproperty
from django.utils.functional import classproperty

def normalize_whitespace(text):

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
    string_subquery = normalize_whitespace(subquery)
    for key, value in subquery.compile().params.items():
        if isinstance(value, str):
            value = f'"{value}"'
        string_subquery = string_subquery.replace(f':{key}', str(value))
    return string_subquery.lstrip('(').rstrip(')')


def load_table(table):
    if isinstance(table, str):
        table = load_object(table)

    if isinstance(table, DeclarativeAttributeIntercept):
        return table.__table__
    elif isinstance(table, Table):
        return table

    raise TypeError()


def load_stmt(stmt):
    if isinstance(stmt, str):
        stmt = load_object(stmt)
    return stmt

