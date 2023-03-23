
from sqlalchemy.orm import sessionmaker
from scrapy_sql import ScrapyDeclarativeBase
from scrapy_sql.subquery_item import SubqueryItem, Field

from sqlalchemy import insert
from sqlalchemy import Column, Date, ForeignKey, Integer, String, Table, Text
from sqlalchemy.orm import sessionmaker, DeclarativeBase, relationship


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


if __name__ == '__main__':

    from datetime import datetime
    from sqlalchemy import create_engine, select

    from scrapy_sql.utils import subquery_to_string
    from pprint import pprint

    kennedy_kwargs = {
        'name': 'John F. Kennedy',
        'birthday': datetime(month=5, day=29, year=1917),
        'bio': '35th president of the United States.'
    }
    kennedy = Author(**kennedy_kwargs)

    change_kwargs = {'name': 'change'}
    deep_thoughts_kwargs = {'name': 'deep-thoughts'}
    change = Tag(**change_kwargs)
    deep_thoughts = Tag(**deep_thoughts_kwargs)

    quote_kwargs = {
        'quote': 'If not us, who? If not now, when?',
        'author': kennedy,
        'tags': [change, deep_thoughts]
    }
    quote = Quote(**quote_kwargs)

    in_memory = 'sqlite:///memory:'
    as_file = 'sqlite:///DB_TESTING.db'
    engine = create_engine(as_file)
    Session = sessionmaker()
    Session.configure(bind=engine, autoflush=False)
    session = Session()

    subquery_string = 'SELECT author.id FROM author WHERE author.name = "Fed"'
    subquery = Author._from_repr_subquery(None, subquery_string)
    print(subquery.text)
    _subquery = select(Author.id).where(Author.name == "Fed")
    print(subquery == _subquery)


    print(None)

    # QuotesBase.metadata.drop_all(engine)
    # QuotesBase.metadata.create_all(engine)

    # session.add(quote)
    # session.commit()



