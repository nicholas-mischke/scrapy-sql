
from sqlalchemy.orm import DeclarativeBase
from scrapy_sql import ScrapyDeclarativeBase

from scrapy_sql.subquery_item import SubqueryItem, Field

from sqlalchemy import Column, Date, ForeignKey, Integer, String, Table, Text
from sqlalchemy.orm import relationship


class QuotesBase(DeclarativeBase, ScrapyDeclarativeBase):
    pass


class Author(QuotesBase):
    __tablename__ = 'author'

    id = Column(Integer, primary_key=True)

    name = Column(String(50), unique=True, nullable=False)
    birthday = Column(Date, nullable=False)
    bio = Column(Text, nullable=False)


class Tag(QuotesBase):
    __tablename__ = 'tag'

    id = Column(Integer, primary_key=True)
    name = Column(String(31), unique=True, nullable=False)


class Quote(QuotesBase):
    __tablename__ = 'quote'

    id = Column(Integer, primary_key=True)
    author_id = Column(ForeignKey('author.id'), nullable=False)

    quote = Column(Text, unique=True, nullable=False)

    author = relationship('Author')
    tags = relationship('Tag', secondary='quote_tag')


t_quote_tag = Table(
    'quote_tag', QuotesBase.metadata,
    Column('quote_id', ForeignKey('quote.id'), primary_key=True),
    Column('tag_id',   ForeignKey('tag.id'),   primary_key=True)
)


class AuthorSubquery(SubqueryItem):
    """Used alongisde Quote to subquery author_id Column"""

    orm_entity = Author
    return_columns = ('id', )

    name = Field()
