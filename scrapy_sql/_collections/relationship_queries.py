class RelationshipQueryIter:

    def __init__(self, query_filter):

        self.query_filter = query_filter

        if query_filter.single_relation:
            self._iterations = 1
            self.data = self.query_filter.data
        else:
            self._iterations = len(query_filter.data)
            # sets aren't subscritable
            self.data = list(self.query_filter.data)

        self._current_index = 0

    def __iter__(self):
        return self

    def __next__(self):
        while self._current_index < self._iterations:
            if self.query_filter.single_relation:
                member = self.data
            else:
                member = self.data[self._current_index]

            self._current_index += 1
            return member

        raise StopIteration


class RelationshipQuery:

    def __init__(self, relationship):
        """
        base_cls (type SQLAlchemy Table):
        relationship_attr (string): the name of the relationship attr in the base_cls
        relationship_info (dict):
        """

        self.cls = relationship.cls
        self.column_names = relationship.column_names
        self.single_relation = relationship.single_relation

        self.data = {} if self.single_relation else set()

    def update(self, column_name, value):

        if column_name not in self.column_names:
            raise TypeError()

        if self.single_relation:
            self.data.update({column_name: value})
        else:
            self.data.add({column_name: value})

    @property
    def isempty(self):
        return len(self.data) == 0

    def __add__(self, other):
        """Given two QueryFilters add them all together"""

    def __str__(self):
        return str(self.data)

    __repr__ = __str__

    def __iter__(self):
        return RelationshipQueryIter(self)


class RelationshipQueryCollectionIter:
    pass


class RelationshipQueryCollection:

    def __init__(self, parent_cls):
        """
        parent_table: the SQLAlchemy Table that hasa QueryFilterContainer
        """
        self.data = {
            r.name: RelationshipQuery(r)
            for r in parent_cls.relationships
        }

    def update(self, attr, value):
        if 'DOT' not in attr:
            raise BaseException()

        attribute_name, attribute_column = attr.split('DOT')
        self.data[attribute_name].update(attribute_column, value)

    def get(self, relationship_attr):
        return self.data[relationship_attr]

    @property
    def isempty(self):
        return all([_.isempty for _ in self.data.values()])

    def __str__(self):
        return str(
            {key: str(value) for key, value in self.data.items()}
        )

    __repr__ = __str__
