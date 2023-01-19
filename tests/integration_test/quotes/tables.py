
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
    ScrapyDeclarativeMetaAdapter,
    connection_info
)
# from scrapy_sql.loader import TableLoader
from scrapy.loader import ItemLoader


database = connection_info.get('quotes')
engine = database.get('engine')
Base = database.get('Base')


class Author(Base, ScrapyDeclarativeMetaAdapter):
    __tablename__ = 'author'

    id = Column(Integer, primary_key=True)

    name = Column(String(50), unique=True)
    birthday = Column(DateTime)
    bio = Column(Text)


class Quote(Base, ScrapyDeclarativeMetaAdapter):
    __tablename__ = 'quote'

    id = Column(Integer, primary_key=True)
    author_id = Column(ForeignKey('author.id'))

    quote = Column(Text)

    author = relationship('Author')
    tags = relationship('Tag', secondary='quote_tag')


class Tag(Base, ScrapyDeclarativeMetaAdapter):
    __tablename__ = 'tag'

    id = Column(Integer, primary_key=True)
    name = Column(String(31), unique=True)


t_quote_tag = Table(
    'quote_tag', Base.metadata,
    Column('quote_id', ForeignKey('quote.id')),
    Column('tag_id', ForeignKey('tag.id'))
)

# Create tables if not exists
Base.metadata.create_all(engine)


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


class SayHello:

    def __call__(self, values):
        input('\n\nHello, Scrapy!\n\n')
        return values


def to_datetime_obj(text):
    # convert string 1879-03-14 to Python date
    return datetime.strptime(text, '%Y-%m-%d').date()


class QuoteLoader(ItemLoader):
    default_item_class = Quote
    default_input_processor = RemoveExcessWhiteSpaces()
    default_output_processor = TakeFirst()

    authorDOTname = MapCompose(
        RemoveExcessWhiteSpaces,
        SayHello
    )
    tags_out = MapCompose(
        TakeAll,
        SayHello
    )


class AuthorLoader(ItemLoader):
    default_item_class = Author
    default_input_processor = RemoveExcessWhiteSpaces()
    default_output_processor = TakeFirst()

    birthday_in = MapCompose(to_datetime_obj)


class TagLoader(ItemLoader):
    default_item_class = Tag
    default_input_processor = RemoveExcessWhiteSpaces()
    default_output_processor = TakeFirst()
