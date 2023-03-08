
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

    einstein = Author(**{
        'name': 'Albert Einstein',
        'birthday': datetime(month=3, day=14, year=1879).date(),
        'bio': 'Won the 1921 Nobel Prize in Physics.'
    })

    instance_classes = tuple(QuotesBase.sorted_entities)

    for x in instance_classes:
        print(x, '\n')

    print(isinstance(einstein, (Author, Tag, Quote)))


    # from sqlalchemy import create_engine, select, text
    # from sqlalchemy.orm import sessionmaker, aliased
    # from pprint import pprint

    # # Connections params
    # uri = 'sqlite:///tests/integration_test/quotes_debug.db'
    # engine = create_engine(uri, echo=True)

    # Session = sessionmaker(**{'bind': engine, 'autoflush': False})
    # session = Session()

    # Base = QuotesBase

    # QuotesBase.metadata.drop_all(engine)
    # QuotesBase.metadata.create_all(engine)

    # einstein = Author(**{
    #     'name': 'Albert Einstein',
    #     'birthday': datetime(month=3, day=14, year=1879).date(),
    #     'bio': 'Won the 1921 Nobel Prize in Physics.'
    # })
    # kennedy = Author(**{
    #     'name': 'JFK',
    #     'birthday': datetime(month=5, day=29, year=1917).date(),
    #     'bio': '35th President.'
    # })

    # change = Tag(**{'name': 'inspirational'})
    # deep_thoughts = Tag(**{'name': 'deep thoughts'})
    # community = Tag(**{'name': 'community'})

    # einstein_quote = Quote(**{
    #     'quote': (
    #         'The world as we have created it is a process of our thinking. '
    #         'It cannot be changed without changing our thinking.'
    #     ),
    #     'author': einstein,
    #     'tags': [change, deep_thoughts]
    # })
    # einstein_quote_II = Quote(**{
    #     'author_id': einstein.subquery('id'),
    #     'quote': (
    #         'The world as we have created it is a process of our thinking. '
    #         'It cannot be changed without changing our thinking.'
    #     ),
    #     'tags': [change, deep_thoughts]
    # })
    # kennedy_quote = Quote(**{
    #     'quote': (
    #         'Ask not what your country can do for you, '
    #         'but what you can do for your country.'
    #     ),
    #     'author': kennedy,
    #     'tags': [community]
    # })

    # einstein_from_repr = Author.from_repr(str(einstein))
    # print(einstein_from_repr == einstein)
    # print(einstein_from_repr.params == einstein.params)

    # session.execute(insert(Author), [einstein_from_repr.params])
    # session.commit()

    # einstein_quote_II_from_repr = Quote.from_repr(str(einstein_quote_II))
    # print(einstein_quote_II_from_repr)
    # print(einstein_quote_II_from_repr == einstein_quote_II)
    # print(einstein_quote_II_from_repr.params == einstein_quote_II.params)

    # session.execute(insert(Quote).values([einstein_quote_II_from_repr.params]))
    # session.commit()


################################################################################
################################################################################
################################################################################

    # # INSERT IGNORE
    # input('INSERT IGNORE or sqlalchemy.exc.IntegrityError depending on if we query')
    # # quote = session.query(Quote).one()
    # session.add(quote)
    # session.commit()

    # # UPDATE
    # input('UPDATE')
    # quote.author = kennedy
    # session.commit()

    # # DELETE
    # input('DELETE')
    # session.delete(quote)
    # session.commit()

    # authors = session.query(Author).all()
    # tags = session.query(Tag).all()
    # quotes = session.query(Quote).all()
    # quote_tags = session.query(t_quote_tag).all()

    # print('\n\n')
    # print('Authors:')
    # for instance in authors:
    #     print(instance)
    # print()

    # print('Tags:')
    # for instance in tags:
    #     print(instance)
    # print()

    # print('Quotes:')
    # for instance in quotes:
    #     print(instance)
    # print()

    # print('t_quote_tag:')
    # for instance in quote_tags:
    #     print(instance)
    # print('\n\n')

    # (deque(state.mapper._props.values())
    # 0: Relationship author
    # 1: Relationship tags
    # 2: Column id
    # 3: Column author_id
    # 4: Column quote

    # # States & Mappers
    # quote_state = instance_state(quote)
    # quote_mapper = quote_state.mapper
    # quote_base_mapper = quote_mapper.base_mapper
