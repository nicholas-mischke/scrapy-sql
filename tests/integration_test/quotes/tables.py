
import re
from datetime import datetime

from itemloaders.processors import (
    Identity,
    TakeFirst,
    MapCompose
)

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Table, Text
from sqlalchemy.orm import relationship

from scrapy_sql import (
    TableLoader,
    ScrapyDeclarativeMetaAdapter,
    databases_info
)


database = databases_info.get('quotes')
engine = database.get('engine')
Base = database.get('Base')
metadata = Base.metadata


class Author(Base, ScrapyDeclarativeMetaAdapter):
    __tablename__ = 'author'

    id = Column(Integer, primary_key=True)

    name = Column(String(50), unique=True)
    birthday = Column(DateTime)
    bio = Column(Text)


class Quote(Base, ScrapyDeclarativeMetaAdapter):
    __tablename__ = 'quote'

    id = Column(Integer, primary_key=True)

    quote = Column(Text)
    author = Column(Text)

    # author_id = Column(ForeignKey('author.id'))
    # author = relationship('Author')
    # tags = relationship('Tag', secondary='quote_tag')


class Tag(Base, ScrapyDeclarativeMetaAdapter):
    __tablename__ = 'tag'

    id = Column(Integer, primary_key=True)
    name = Column(String(31), unique=True)


t_quote_tag = Table(
    'quote_tag', metadata,
    Column('quote_id', ForeignKey('quote.id')),
    Column('tag_id', ForeignKey('tag.id'))
)

# Create tables if not exists
metadata.create_all(engine)


class TakeAll():
    """
    ItemLoader processors that returns the list
    the ItemLoader holds internally.
    """

    def __call__(self, values):
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


class QuoteLoader(TableLoader):
    default_item_class = Quote
    default_input_processor = RemoveExcessWhiteSpaces()
    default_output_processor = TakeFirst()

    tags_out = TakeAll()


class AuthorLoader(TableLoader):
    default_item_class = Author
    default_input_processor = RemoveExcessWhiteSpaces()
    default_output_processor = TakeFirst()

    birthday_in = MapCompose(to_datetime_obj)


class TagLoader(TableLoader):
    default_item_class = Tag
    default_input_processor = RemoveExcessWhiteSpaces()
    default_output_processor = TakeFirst()
