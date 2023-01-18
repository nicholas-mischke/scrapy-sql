from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Table, Text
from sqlalchemy.orm import relationship

from scrapy_sql import (
    ScrapyDeclarativeMetaAdapter
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.decl_api import DeclarativeMeta

# engine = create_engine('sqlite:///:memory:')
engine = create_engine('sqlite:///quotes_playground.db')
Base = declarative_base()
metadata = Base.metadata

Session = sessionmaker()
Session.configure(bind=engine)
session = Session()


def existing_table(session, table):
    return session.query(
        table.__class__
    ).filter_by(
        **table.asdict()
    ).first() or table

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


# Create tables if not exists
Base.metadata.create_all(engine)



# # Generate Authors
# einstein = Author()
# einstein.name, einstein.birthday, einstein.bio = (
#     'Albert Einstein',
#     datetime(year=1879, month=3, day=14),
#     'Won the 1921 Nobel Prize in Physics for his paper explaining the photoelectric effect.'
# )
# session.add(
#     existing_table(session, einstein)
# )

# # Generate Quotes
# einstein_quote_I = Quote()
# einstein_quote_I.quote = 'The world as we have created it is a process of our thinking. It cannot be changed without changing our thinking.'

# einstein_quote_I.__author_filter_kwargs = {'name': 'Albert Einstein'}
# einstein_quote_I.set_relationships(session)
# session.add(
#     existing_table(session, einstein_quote_I)
# )

# session.commit()


