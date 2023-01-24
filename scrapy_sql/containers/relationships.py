
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.inspection import inspect

from functools import cached_property


class Relationship:
    def __init__(self, item_class, relationship):
        self.item_class = item_class

        self.name = relationship.class_attribute.key
        self.cls = relationship.mapper.class_
        self.columns = self.cls.__table__.columns

        self.direction = relationship.direction.name
        self.related_tables = getattr(
            self.item_class,
            self.name
        )

        self.single_relation = not isinstance(
            self.related_tables,
            InstrumentedList
        )

    @cached_property
    def accepted_DOT_field_names(self):
        return tuple(
            f'{self.name}DOT{column.name}'
            for column in self.columns
        )

    def __len__(self):
        if self.single_relation:
            return 0 if self.related_tables is None else 1
        return len(self.related_tables)

    def __iter__(self):  # Iterate tables in relationship
        if self.single_relation:
            if self.related_tables is None:
                related_tables = []
            else:
                related_tables = [self.related_tables]
        else:
            related_tables = self.related_tables

        return iter(related_tables)


class RelationshipCollection:

    def __init__(self, item_class):

        self.relationships = tuple(
            Relationship(item_class, r)
            for r in inspect(item_class).relationships
        )
        self.relationships_dict = {
            r.name: r for r in self.relationships
        }

    @cached_property
    def names(self):
        return tuple(r.name for r in self.relationships)

    @cached_property
    def accepted_DOT_field_names(self):
        result = tuple()
        for relationship in self:
            result += relationship.accepted_DOT_field_names
        return result

    def __getitem__(self, relationship_name):
        return self.relationships_dict[relationship_name]

    def __iter__(self):
        return iter(self.relationships)
