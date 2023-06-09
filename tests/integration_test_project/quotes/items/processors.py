
import re
from datetime import datetime


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


class StringToDate(InputProcessor):

    def __init__(self, format='%Y-%m-%d'):
        self.format = format

    def process_value(self, value):
        return datetime.strptime(value, self.format).date()


class StringToTime(InputProcessor):

    def __init__(self, format='%H:%M:%S'):
        self.format = format

    def process_value(self, value):
        return datetime.strptime(value, self.format).time()


class StringToDateTime(InputProcessor):

    def __init__(self, format='%Y-%m-%d %H:%M:%S'):
        self.format = format

    def process_value(self, value):
        return datetime.strptime(value, self.format)


class TakeAll:

    def __call__(self, values):
        if not isinstance(values, list):
            raise TypeError()
        return values if len(values) > 0 else None
