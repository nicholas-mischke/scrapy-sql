
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.inspection import inspect


def is_single_relation(relationship):
    return not hasattr(relationship, '__iter__')


class RelationshipIter:

    def __init__(self, relationship):
        self.relationship = relationship
        self._num_tables = len(relationship)
        self._current_index = 0

    def __iter__(self):
        return self

    def __next__(self):
        while self._current_index < self._num_tables:
            if self.relationship.single_relation:
                member = self.relationship.related_tables
            else:
                member = self.relationship.related_tables[self._current_index]

            self._current_index += 1
            return member

        raise StopIteration


class Relationship:
    def __init__(self, declarative_meta_instance, relationship):
        self.declarative_meta_instance = declarative_meta_instance

        self.name = relationship.class_attribute.key
        self.cls = relationship.mapper.class_
        self.column_names = self.cls().column_names

        self.direction = relationship.direction.name
        self.related_tables = getattr(
            self.declarative_meta_instance,
            self.name
        )

        self.single_relation = not isinstance(
            self.related_tables,
            InstrumentedList
        )

        # self.query_filter = self.parent_cls.query_filters.get(self.name)

    def replace_tables(self, tables):
        if (
            self.single_relation
            and isinstance(tables, self.cls)
        ):
            pass
        elif (
            self.single_relation
            and isinstance(tables, InstrumentedList)
        ):
            if len(tables) == 0:
                tables = None
            elif len(tables) == 1:
                tables = tables[0]
        elif (
            not self.single_relation
            and isinstance(tables, self.cls)
        ):
            tables = InstrumentedList([tables])

        if (
            not self.single_relation
            and isinstance(tables, (InstrumentedList, list, tuple))
            and all([isinstance(t, self.cls) for t in tables])
        ):
            pass

        setattr(self.parent_cls, self.name, tables)

    def extend(self, tables):
        if self.single_relation:
            raise BaseException()

        tables = InstrumentedList(arg_to_iter(tables))

        if all([isinstance(t, self.cls) for t in tables]):
            getattr(self.parent_cls, self.name).extend(tables)

    def __len__(self):
        if (
            self.single_relation
            and self.related_tables is None
        ):
            return 0
        elif (
            self.single_relation
        ):
            return 1
        return len(self.related_tables)

    def __iter__(self):
        return RelationshipIter(self)
