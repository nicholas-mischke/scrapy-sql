
import copy
from sqlalchemy.orm import sessionmaker, persistence  # , Session
from sqlalchemy import event
from sqlalchemy import (
    insert, select,
    Column, Date, ForeignKey, Integer, String, Table, Text
)
from sqlalchemy.orm import DeclarativeBase, relationship
from scrapy_sql import ScrapyDeclarativeBase
from descriptors import classproperty, cachedclassproperty
from sqlalchemy.inspection import inspect
from sqlalchemy.util._py_collections import immutabledict

from scrapy.utils.python import flatten
from scrapy.utils.misc import arg_to_iter
from sqlalchemy.orm.base import ONETOMANY, MANYTOONE, MANYTOMANY


# Project Imports
from scrapy_sql.utils import column_value_is_subquery

class QuotesBase(DeclarativeBase, ScrapyDeclarativeBase):
    pass


class Author(QuotesBase):
    __tablename__ = 'author'

    id = Column(Integer, primary_key=True)

    name = Column(String(50), unique=True, nullable=False)
    birthday = Column(Date, nullable=False)
    bio = Column(Text, nullable=False)

    @classproperty
    def stmt(cls):
        return insert(Author).prefix_with('OR IGNORE')


class Tag(QuotesBase):
    __tablename__ = 'tag'

    id = Column(Integer, primary_key=True)
    name = Column(String(31), unique=True, nullable=False)

    @classproperty
    def stmt(cls):
        return insert(Tag).prefix_with('OR IGNORE')


class Quote(QuotesBase):
    __tablename__ = 'quote'

    id = Column(Integer, primary_key=True)
    author_id = Column(ForeignKey('author.id'), nullable=False)

    quote = Column(Text, unique=True, nullable=False)

    author = relationship('Author')
    tags = relationship('Tag', secondary='quote_tag')

    @classproperty
    def stmt(cls):
        return insert(Quote).prefix_with('OR IGNORE')


t_quote_tag = Table(
    'quote_tag', QuotesBase.metadata,
    Column('quote_id', ForeignKey('quote.id'), primary_key=True),
    Column('tag_id',   ForeignKey('tag.id'),   primary_key=True)
)
setattr(t_quote_tag, 'stmt', insert(t_quote_tag).prefix_with('OR IGNORE'))


if __name__ == '__main__':

    from datetime import datetime
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from pprint import pprint
    from copy import deepcopy

    from sqlalchemy.orm.attributes import instance_state

    einstein = Author(**{
        'name': 'Albert Einstein',
        'birthday': datetime(month=3, day=14, year=1879),
        'bio': 'Won the 1921 Nobel Prize in Physics.'
    })
    kennedy = Author(**{
        'name': 'JFK',
        'birthday': datetime(month=5, day=29, year=1917),
        'bio': '35th President.'
    })

    change = Tag(**{'name': 'inspirational'})
    deep_thoughts = Tag(**{'name': 'deep thoughts'})
    community = Tag(**{'name': 'community'})

    einstein_quote = Quote(**{
        'quote': (
            'The world as we have created it is a process of our thinking. '
            'It cannot be changed without changing our thinking.'
        ),
        'author': einstein,
        'tags': [change, deep_thoughts]
    })
    kennedy_quote = Quote(**{
        'quote': (
            'Ask not what your country can do for you, '
            'but what you can do for your country.'
        ),
        'author': kennedy,
        'tags': [community]
    })

################################################################################

    # Connections params
    uri = 'sqlite:///tests/integration_test/quotes_debug.db'
    engine = create_engine(uri, echo=True)

    Session = sessionmaker(**{'bind': engine, 'autoflush': False})
    session = Session()

    QuotesBase.metadata.drop_all(engine)
    QuotesBase.metadata.create_all(engine)

################################################################################
# Generate Subqueries

    class ManyToOneBulkDP:

        def __init__(self, instance, relationship):
            self.instance = instance
            self.relationship = relationship
            self.related_instance = getattr(
                self.instance,
                self.relationship.class_attribute.key
            )

        def prepare(self):
            """
            Prepare an instance with a ManyToOne relationship for bulk insert.

            If the foreign key column is already populated, make no change.

            If the foreign key column is None and the remote column has a value,
            assign the local column the value of the remote column

            If both the local and remote columns have values of None,
            generate a subquery to populate the value while inserting.
            """

            for pair in self.relationship.local_remote_pairs:
                local_column, remote_column = pair

                local_value = getattr(self.instance, local_column.name)
                if local_value is not None:
                    continue  # go to next pair

                remote_value = getattr(
                    self.related_instance,
                    remote_column.name
                )
                if remote_value is not None:
                    setattr(self.instance, local_column.name, remote_value)
                    continue

                setattr(
                    self.instance,
                    local_column.name,
                    self.related_instance.subquery(remote_column)
                )

    class ManyToManyBulkDP:
        def __init__(self, instance, relationship):
            self.instance = instance
            self.relationship = relationship
            self.related_instances = getattr(
                self.instance,
                self.relationship.class_attribute.key
            )
            self.secondary = relationship.secondary

        def determine_join_table_column_value(
            self,
            parent_instance,
            parent_column,
            join_table_column
        ):
            d = {}

            parent_value = getattr(parent_instance, parent_column.name)
            if parent_value is not None:
                d[join_table_column.name] = parent_value
            else:
                d[join_table_column.name] = parent_instance \
                    .subquery(parent_column)

            return d

        def prepare_secondary(self):
            """
            Determine the subqueries necessary to insert the columns
            of a join table
            """
            params = []

            param = {}
            for pair in self.relationship.synchronize_pairs:
                parent_column, join_table_column = pair
                param.update(
                    self.determine_join_table_column_value(
                        self.instance,
                        parent_column,
                        join_table_column
                    )
                )

            for related_instance in self.related_instances:

                secondary_param = deepcopy(param)

                for pair in self.relationship.secondary_synchronize_pairs:
                    parent_column, join_table_column = pair
                    secondary_param.update(
                        self.determine_join_table_column_value(
                            related_instance,
                            parent_column,
                            join_table_column
                        )
                    )

                params.append(secondary_param)

            print('Breakpoint')

            return params

    session.add(einstein_quote)
    session.add(kennedy_quote)

    sorted_tables = QuotesBase.metadata.sorted_tables

    table_statements = {
        table: insert(table).prefix_with('OR IGNORE')
        for table in sorted_tables
    }

    table_params = {
        table: []
        for table in sorted_tables
    }

    for instance in session:
        mapper = instance_state(instance).mapper

        for r in mapper.relationships:

            if r.direction is MANYTOONE:
                ManyToOneBulkDP(instance, r).prepare()

            elif r.direction is MANYTOMANY:
                dependency_processor = ManyToManyBulkDP(instance, r)
                table_params[dependency_processor.secondary].extend(
                    dependency_processor.prepare_secondary()
                )

        table_params[instance.__table__].append(instance.params)

    # UOW INSERTs / UPSERTs occur on session.commit()
    # We're only interested in BULK INSERTs / UPSERTs here
    session.expunge_all()

    for table in sorted_tables:
        statement = table_statements[table]
        params = table_params[table]

        contains_subqueries = any([
            column_value_is_subquery(value)
            for value in flatten([x.values() for x in params])
        ])

        if contains_subqueries:
            session.execute(statement.values(params))
        else:
            session.execute(statement, params)

        session.commit()


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
