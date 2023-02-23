
import re
from datetime import datetime

from .models import Author
from sqlalchemy import select
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.attributes import ScalarObjectAttributeImpl
from sqlalchemy.orm.attributes import instance_state


class InputProcessor:

    def process_value(self, value):
        raise NotImplementedError()

    def __call__(self, values):
        return [self.process_value(value) for value in values]


class MapCompose(InputProcessor):

    def __init__(self, *args):
        self.processors = args

    def process_value(self, value):
        for processor in self.processors:
            value = processor.process_value(value)


class RemoveExcessWhitespace(InputProcessor):
    """
    "\t\nHello          \nWorld" --> "Hello World"
    """

    # Turn multiple whitespaces into single whitespace
    RE_COMBINE_WHITESPACE = re.compile(r"(?a:\s+)")
    RE_STRIP_WHITESPACE = re.compile(r"(?a:^\s+|\s+$)")

    def process_value(self, value):

        if not isinstance(value, str):
            raise TypeError()

        value = value.encode('ascii', 'ignore').decode('utf-8')
        value = RemoveExcessWhitespace.RE_COMBINE_WHITESPACE.sub(" ", value)
        value = RemoveExcessWhitespace.RE_STRIP_WHITESPACE.sub("", value)
        return value


class StringToDate(InputProcessor):

    def __init__(self, format='%Y-%m-%d'):
        self.format = format

    def process_value(self, value):
        return datetime.strptime(value, self.format).date()


class QueryAuthor(InputProcessor):

    def process_value(self, value):
        return select(Author.id).where(Author.name == value).scalar_subquery()


class SubQueryPrimaryKey(InputProcessor):

    def process_value(self, instance):

        columns = instance.__class__.__table__.columns
        state = instance_state(instance)
        mapper = state.mapper

        primary_key = mapper.primary_key

        query_tuples = []
        for column in columns:
            pass

        primary_keys = mapper._all_pk_cols

        select(primary_key).where(

        )

        primary_key = None
        if isinstance(primary_key, ScalarObjectAttributeImpl):
            pass

        filter_columns = []

        for column in columns:

            column_value = getattr(instance, column.name)

            if column.primary_key is True:
                primary_key.append(column)
            elif column_value is None:
                continue
            elif column_value.is_clause_element:
                continue
            elif column.foreign_keys != set():
                continue
            else:
                filter_columns.append({column: column_value})

        # return select(*primary_keys).where()

        # return f"`````{value}`````"
        # return select(*primary_keys).where(Author.name == value)


class TakeAll:

    def __call__(self, values):
        return values if len(values) > 0 else None
