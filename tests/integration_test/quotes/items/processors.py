
import re
from datetime import datetime

# from .models import Author
# from sqlalchemy import select
# from sqlalchemy.orm.base import object_mapper


class InputProcessor:

    def process_value(self, value):
        raise NotImplementedError()

    def __call__(self, values):
        return [self.process_value(value) for value in values]


class RemoveExcessWhitespace(InputProcessor):
    """
    "\t\nHello          \nWorld     " --> "Hello World"
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


class LowerCase(InputProcessor):

    def process_value(self, value):
        if not isinstance(value, str):
            raise TypeError()
        return value.lower()


class UpperCase(InputProcessor):

    def process_value(self, value):
        if not isinstance(value, str):
            raise TypeError()
        return value.upper()


class TitleCase(InputProcessor):

    def process_value(self, value):
        if not isinstance(value, str):
            raise TypeError()
        return value.title()


class StringToDate(InputProcessor):

    def __init__(self, format='%Y-%m-%d'):
        self.format = format

    def process_value(self, value):
        return datetime.strptime(value, self.format).date()


class TakeAll:

    def __call__(self, values):
        return values if len(values) > 0 else None


if __name__ == '__main__':
    print(StringToDate().process_value('1879-03-14'))
    print(RemoveExcessWhitespace().process_value(
        '\t\nHello          \nWorld!     '))
