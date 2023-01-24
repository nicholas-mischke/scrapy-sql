
from sqlalchemy import Column, Date, ForeignKey, Integer, String, Table, Text
from sqlalchemy.orm import relationship

from sqlalchemy.ext.declarative import declarative_base
from scrapy_sql import ScrapyDeclarativeMetaAdapter


QuotesBase = declarative_base()


class Author(QuotesBase, ScrapyDeclarativeMetaAdapter):
    __tablename__ = 'author'

    id = Column(Integer, primary_key=True)

    name = Column(String(50), unique=True)
    birthday = Column(Date)
    bio = Column(Text)


class Quote(QuotesBase, ScrapyDeclarativeMetaAdapter):
    __tablename__ = 'quote'

    id = Column(Integer, primary_key=True)
    author_id = Column(ForeignKey('author.id'))

    quote = Column(Text)

    author = relationship('Author')
    tags = relationship('Tag', secondary='quote_tag')


class Tag(QuotesBase, ScrapyDeclarativeMetaAdapter):
    __tablename__ = 'tag'

    id = Column(Integer, primary_key=True)
    name = Column(String(31), unique=True)


t_quote_tag = Table(
    'quote_tag', QuotesBase.metadata,
    Column('quote_id', ForeignKey('quote.id')),
    Column('tag_id', ForeignKey('tag.id'))
)


if __name__ == '__main__':

    from scrapy_sql import SQLAlchemyTableAdapter
    from pprint import pprint

    quote = Quote(
        **{
            'id': 1,
            'author_id': 1,
            'quote': 'If not us, who? If not now, when?'
        }
    )
    adapter = SQLAlchemyTableAdapter(quote)

    pprint(adapter.asdict())
