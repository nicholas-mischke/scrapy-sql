
# from scrapy_sql.scrapy_session import ScrapySession
import re
from datetime import datetime
from sqlalchemy.orm.decl_api import DeclarativeMeta
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
from sqlalchemy.orm.collections import InstrumentedList
from scrapy_sql.tableadapter import SQLAlchemyTableAdapter

# from scrapy_sql.loader import TableLoader
from scrapy.loader import ItemLoader

from pathlib import Path
# /mnt/4TB-HDD/Dropbox/Programming/python_packages/scrapy-sql/development/tests/playground.py

db_file = 'quotes_playground.db'
conn_str = f'sqlite:///{db_file}'
# input(conn_str)

engine = create_engine(conn_str)
Base = declarative_base()

Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

# session = ScrapySession(Base, engine)


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

# Load up the first quote from the website
einstein = Author()
einstein.name, einstein.birthday, einstein.bio = (
    'Albert Einstein',
    datetime(month=3, day=14, year=1879),
    'Won the 1921 Nobel Prize in Physics.'
)

change = Tag()
change.name = 'change'

change_duplicate = Tag()
change_duplicate.name = 'change'

deep_thoughts = Tag()
deep_thoughts.name = 'deep-thoughts'

quote = Quote()
quote.quote, quote.author = (
    'The world as we have created it is a process of our thinking. It cannot be changed without changing our thinking.',
    einstein
)
quote.tags.extend([change, deep_thoughts])
print(quote)




# print(change == change_duplicate)
# print(change is change_duplicate)

# session.add(einstein)
# session.add(change)
# session.add(deep_thoughts)
# session.add(quote)

# session.commit()
# session.close()
