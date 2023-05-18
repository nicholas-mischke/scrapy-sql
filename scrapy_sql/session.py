
# Project Imports
from .utils import column_value_is_subquery, subquery_to_string, load_table, load_stmt

# Scrapy / Twisted Imports
from scrapy.utils.misc import load_object
from scrapy.utils.python import flatten, get_func_args

# SQLAlchemy Imports
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import instance_state
from sqlalchemy.orm.base import ONETOMANY, MANYTOONE, MANYTOMANY  # ONETOONE not listed


# 3rd 🎉 Imports
from copy import deepcopy
import logging


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
        
        if self.related_instance is None:
            return
        
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
        return {
            join_table_column.name :
            getattr(parent_instance, parent_column.name) # parent value if present
            or parent_instance.subquery(parent_column)   # else subquery
        }

    def prepare_secondary(self):
        """
        Determine the subqueries necessary to insert the columns
        of a join table
        """
        
        if len(self.related_instances) == 0:
            return
        
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

        return params


class ScrapyBulkSession(Session):

    def __init__(self, autoflush=False, *args, feed_options=None, **kwargs):

        self.Base = load_object(feed_options['declarative_base'])
        self.sorted_tables = self.Base.sorted_tables

        self.orm_stmts = feed_options['orm_stmts']

        super().__init__(autoflush=autoflush, *args, **kwargs)
    

    def bulk_commit(self):

        table_params = {
            table: []
            for table in self.sorted_tables
        }

        # Best for logging
        table_instances = {
            table: []
            for table in self.sorted_tables
        }

        for instance in self:
            mapper = instance_state(instance).mapper

            for r in mapper.relationships:

                # TODO add ONETOONE & ONETOMANY

                if r.direction is MANYTOONE:
                    ManyToOneBulkDP(instance, r).prepare()

                elif r.direction is MANYTOMANY:
                    dependency_processor = ManyToManyBulkDP(instance, r)
                    secondary_table = dependency_processor.secondary
                    join_columns = dependency_processor.prepare_secondary()
                    
                    if join_columns is None:
                        continue

                    table_params[secondary_table].extend(join_columns)
                    table_instances[secondary_table].extend([tuple(d.values()) for d in join_columns])

            table_params[instance.__table__].append(instance.params)
            table_instances[instance.__table__].append(str(instance))
        
        # Logging
        for table in self.sorted_tables:
            try:
                stmt = self.orm_stmts[table](table)
            except TypeError:
                stmt = self.orm_stmts[table](table, self)
            instances = table_instances[table]
            
            if len(instances) == 0:
                continue

            try:
                instances_string = '\n'.join(instances)
            except: # Join Table

                def tuple_of_subqeurires(tup):
                    my_list = []
                    for value in tup:
                        value = subquery_to_string(value) if column_value_is_subquery(value) else value
                        my_list.append(value)
                    return str(tuple(my_list))
                
                instances_string = '\n'.join([tuple_of_subqeurires(tup) for tup in instances])

            logging.info(f"{stmt}\n{instances_string}")
   
        # UOW INSERTs / UPSERTs occur on self.commit()
        # We're only interested in BULK INSERTs / UPSERTs here
        # Possibly start a new transaction and commit it instead
        # of self.commit()
        self.expunge_all()

        for table in self.sorted_tables:
            try:
                stmt = self.orm_stmts[table](table)
            except TypeError:
                stmt = self.orm_stmts[table](table, self)
            
            params = table_params[table]
            if not params:
                continue

            contains_subqueries = any([
                column_value_is_subquery(value)
                for value in flatten([x.values() for x in params])
            ])

            if contains_subqueries:
                self.execute(stmt.values(params))
            else:
                self.execute(stmt, params)

            self.commit() # INSERT rows table by table in sorted order
