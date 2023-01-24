
import re
from datetime import datetime


class TakeAll():
    """
    TableLoader processors that returns the list
    the TableLoader holds internally.
    """

    def __call__(self, values):
        if len(values) == 0:
            return None
        return values


class RemoveExcessWhiteSpaces():
    """
    "\t\nHello          \nWorld" --> "Hello World"
    """

    def __call__(self, values):
        # Turn multiple whitespaces into single whitespace
        _RE_COMBINE_WHITESPACE = re.compile(r"(?a:\s+)")
        _RE_STRIP_WHITESPACE = re.compile(r"(?a:^\s+|\s+$)")

        def remove_excess_whitespaces(value):
            value = str(value)
            value = _RE_COMBINE_WHITESPACE.sub(" ", value)
            return _RE_STRIP_WHITESPACE.sub("", value)

        return [remove_excess_whitespaces(value) for value in values]


def to_datetime_obj(text):
    # convert string 1879-03-14 to Python date
    return datetime.strptime(text, '%Y-%m-%d').date()
