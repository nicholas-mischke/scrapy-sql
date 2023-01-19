
import re
from datetime import datetime

from itemloaders.processors import (
    Identity,
    TakeFirst,
    MapCompose
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, DateTime, ForeignKey, Integer, String, Table, Text
from sqlalchemy.orm import relationship

from scrapy_sql import (
    ScrapyDeclarativeMetaAdapter,
    connection_info
)

from scrapy_sql.tableadapter import SQLAlchemyTableAdapter

# from scrapy_sql.loader import TableLoader
from scrapy.loader import ItemLoader

engine = create_engine('sqlite://')
Base = declarative_base()

Session = sessionmaker()
Session.configure(bind=engine)
session = Session()


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

change = Tag()
change.name = 'change'

change_II = Tag()
change.name = 'change'

session.add(change)
session.add(change)
session.add(change)
session.add(change)
session.commit()

session.add(change_II)
session.commit()


# from pprint import pprint

# author = Author()
# author.name = 'Fat Albert'
# session.add(author)

# quote = Quote()
# quote.quote = 'Hey, Hey, Hey!'
# adapter = SQLAlchemyTableAdapter(quote)
# print(quote)
# adapter['authorDOTname'] = 'Fat Albert'

# quote.query_relationships(session)
# print(quote)