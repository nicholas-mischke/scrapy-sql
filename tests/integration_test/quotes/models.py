
from sqlalchemy import (
    insert, select,
    Column, Date, ForeignKey, Integer, String, Table, Text
)
from sqlalchemy.orm import DeclarativeBase, relationship
from scrapy_sql import ScrapyDeclarativeBaseExtension
from descriptors import classproperty, cachedclassproperty


class QuotesBase(DeclarativeBase):
    pass


class Author(QuotesBase, ScrapyDeclarativeBaseExtension):
    __tablename__ = 'author'

    id = Column(Integer, primary_key=True)

    name = Column(String(50), unique=True, nullable=False)
    birthday = Column(Date, nullable=False)
    bio = Column(Text, nullable=False)


class Tag(QuotesBase, ScrapyDeclarativeBaseExtension):
    __tablename__ = 'tag'

    id = Column(Integer, primary_key=True)
    name = Column(String(31), unique=True, nullable=False)


class Quote(QuotesBase, ScrapyDeclarativeBaseExtension):
    __tablename__ = 'quote'

    id = Column(Integer, primary_key=True)
    author_id = Column(ForeignKey('author.id'), nullable=False)

    quote = Column(Text, nullable=False, unique=True)

    author = relationship('Author')
    tags = relationship('Tag', secondary='quote_tag')


t_quote_tag = Table(
    'quote_tag', QuotesBase.metadata,
    Column('quote_id', ForeignKey('quote.id'), primary_key=True),
    Column('tag_id',   ForeignKey('tag.id'),   primary_key=True)
)


if __name__ == '__main__':
    from sqlalchemy.orm.decl_api import DeclarativeAttributeIntercept

    item_class = Author
    accepted_classes = (DeclarativeAttributeIntercept, )

    print(type(item_class) in accepted_classes)
    print(isinstance(type(item_class()), accepted_classes))
