
import pytest
from datetime import datetime

from sqlalchemy import (
    create_engine,
    Column, DateTime, ForeignKey, Integer, String, Table, Text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm.collections import InstrumentedList

from scrapy_sql import ScrapyDeclarativeMetaAdapter
from sqlalchemy import Table
from sqlalchemy.orm.decl_api import DeclarativeMeta
import scrapy
from scrapy_sql import SQLAlchemyTableAdapter, QueryFilter
from pprint import pprint
from sqlalchemy import func

from sqlalchemy.sql.functions import Function


engine = create_engine('sqlite://')
Base = declarative_base()


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


kennedy = Author()
kennedy.id, kennedy.name, kennedy.birthday, kennedy.bio = (
    1,
    'John F. Kennedy',
    datetime(month=5, day=29, year=1917),
    '35th president of the United States.'
)

change = Tag()
change.id, change.name = (1, 'change')

kennedy_quote = Quote()
kennedy_quote.id, kennedy_quote.author_id, kennedy_quote.quote, kennedy_quote.author, kennedy_quote.tags = (
    1,
    1,
    'If not us, who? If not now, when?',
    kennedy,
    InstrumentedList([change])
)


@pytest.fixture
def author():
    return kennedy


@pytest.fixture
def tag():
    return change


@pytest.fixture
def quote():
    kennedy_quote = Quote()
    kennedy_quote.quote = (
        'If not us, who? If not now, when?',
    )
    return kennedy_quote


@pytest.fixture
def adapted_quote(quote):
    return SQLAlchemyTableAdapter(quote)


class TestScrapyDeclarativeMetaAdapter:

    # def test_columns(self):
    #     pass

    def test_column_names(self, quote):
        """
        since self.__table__.columns is built into SQLAlchemy and likely
        already tested we won't test property columns directly.
        Instead this test will cover both properties
        """
        assert quote.column_names == ('id', 'author_id', 'quote')

    # def test_query_filter(self, author):
    #     Session = sessionmaker()
    #     Session.configure(bind=engine)
    #     session = Session()

    #     filter = author.query_filter
    #     del filter['birthday']

    #     print('\n\n')
    #     print(author.__class__)
    #     pprint(author.query_filter)
    #     print('\n\n')

    #     assert session.query(
    #         author.__class__
    #     ).filter_by(
    #         **filter
    #     ).first() is None

    #     session.add(author)
    #     session.commit()

    #     assert author in session

    #     assert session.query(
    #         author.__class__
    #     ).filter_by(
    #         **filter
    #     ).first() == author

    @pytest.mark.parametrize(
        "quote",
        [
            (kennedy_quote)
        ]
    )
    def test_relationships(self, quote):

        # Replace 'query_filter'
        relationships = quote.relationships
        for name, info in relationships.items():
            info['query_filter'] = None

        assert relationships == {
            'author': {
                'cls': Author,
                'direction': 'MANYTOONE',
                'query_filter': None,
                'related_tables': kennedy,
                'single_relation': True
            },
            'tags': {
                'cls': Tag,
                'direction': 'MANYTOMANY',
                'query_filter': None,
                'related_tables': InstrumentedList([change]),
                'single_relation': False
            }
        }

        assert getattr(
            quote,
            '__author_query_filter'
        )
        assert isinstance(getattr(quote, '__author_query_filter'), QueryFilter)

    # def test_query_relationships(self):
    #     pass

    # def test_filter_relationships(self):
    #     pass

    def test_accepted_DOT_field_names(self, quote):
        assert quote.accepted_DOT_field_names == (
            'authorDOTid', 'authorDOTname', 'authorDOTbirthday', 'authorDOTbio',
            'tagsDOTid', 'tagsDOTname'
        )

    # def test_asdict(self, einstein):
    #     assert einstein.asdict() == {
    #         'id': 1,
    #         'name': 'Albert Einstein',
    #         'birthday': datetime(year=1879, month=3, day=14),
    #         'bio': 'Won the 1921 Nobel Prize in Physics for his paper explaining the photoelectric effect.'
    #     }

    # def test_dunder_repr(self, einstein):
    #     assert einstein.__repr__() == (
    #         "Author(id=1, "
    #         "name='Albert Einstein', "
    #         "birthday=1879-03-14 00:00:00, "
    #         "bio='Won the 1921 Nobel Prize in Physics for his paper explaining "
    #         "the photoelectric effect.')"
    #     )

    # def test_dunder_str(self, einstein):
    #     assert einstein.__str__() == (
    #         "Author(id=1, "
    #         "name='Albert Einstein', "
    #         "birthday=1879-03-14 00:00:00, "
    #         "bio='Won the 1921 Nobel Prize in Physics for his paper explaining "
    #         "the photoelectric effect.')"
    #     )


# class TestSQLAlchemyTableAdapter:

#     # class methods
#     @pytest.mark.parametrize(
#         "item_class, expected",
#         [
#             (Table, True),
#             (DeclarativeMeta, True),
#             (dict, False),
#             (scrapy.Item, False)
#         ]
#     )
#     def test_is_item_class(self, item_class, expected):
#         """Return True if the adapter can handle the given item class, False otherwise."""
#         assert SQLAlchemyTableAdapter.is_item_class(item_class) == expected

#     def test_is_item_class_II(self, db_tables):
#         for table in db_tables:
#             assert SQLAlchemyTableAdapter.is_item_class(table) == True

#     def test_is_item(self, einstein):
#         assert SQLAlchemyTableAdapter.is_item(einstein) == True

#     def test_get_field_names_from_class(self, author_table):
#         assert SQLAlchemyTableAdapter.get_field_names_from_class(
#             author_table
#         ) == [
#             'id',
#             'name',
#             'birthday',
#             'bio'
#         ]

#     def test_field_names(self, einstein_tableadapter):
#         einstein_tableadapter.field_names == [
#             'id',
#             'name',
#             'birthday',
#             'bio'
#         ]

#     def test_asdict(self, einstein_tableadapter):
#         assert einstein_tableadapter.asdict() == {
#             'id': 1,
#             'name': 'Albert Einstein',
#             'birthday': datetime(year=1879, month=3, day=14),
#             'bio': 'Won the 1921 Nobel Prize in Physics for his paper explaining the photoelectric effect.'
#         }

#     # _MixinColumnSQLAlchemyAdapter
#     def test_dunder_getitem(self, einstein_tableadapter):
#         for attr, value in [
#             ('id', 1),
#             ('name', 'Albert Einstein'),
#             ('birthday', datetime(year=1879, month=3, day=14)),
#             ('bio', 'Won the 1921 Nobel Prize in Physics for his paper explaining the photoelectric effect.')
#         ]:
#             assert einstein_tableadapter[attr] == value

#     def test_dunder_setitem(self, einstein_tableadapter):
#         assert einstein_tableadapter['id'] == 1
#         einstein_tableadapter['id'] = 2
#         assert einstein_tableadapter['id'] == 2

#         # Make einstein a function scope fixture
#         einstein_tableadapter['id'] = 1

#     def test_dunder_delitem(self, einstein_tableadapter):
#         """
#         del einstein_tableadapter['column']
#         should do nothing
#         """
#         del einstein_tableadapter['id']
#         assert 'id' in einstein_tableadapter.asdict() # does not delete

#     def test_dunder_iter(self, einstein_tableadapter):
#         item_asdict = einstein_tableadapter.item.asdict()
#         for key, value in einstein_tableadapter.items():
#             assert item_asdict[key] == value

#     def test_dunder_len(self, einstein_tableadapter):
#         assert len(einstein_tableadapter) == 4
