
from scrapy import Item, Field


class SubqueryItem(Item):
    """
    Used alongside ScrapyDeclarativeBase subclasses for orm_entities.

    The return_columns will be used to SELECT columns from a SQL table
    represented by the orm_entity.
    If return_columns = tuple() the primary keys of the table will be returned.

    The Field properties will be used in a WHERE clause of the SELECT statment

    e.g.)
        class SubqueryItemSubclass(SubqueryItem):
            orm_entity = Entity
            return_columns = ('id', )

            name = Field()

        item = SubqueryItemSubclass()
        item['name'] =  "Fat Albert"
        print(SubqueryItemSubclass().subquery())
        >>> 'SELECT entity.id FROM entity WHERE entity.name = "Fat Albert"'
    """

    orm_entity = None
    return_columns = tuple()

    def __init__(self, *args, **kwargs):
        """
        Simply verifies that the column names supplied in
        return_columns are column names of the orm_entity
        """
        legal_column_names = type(self).orm_entity.column_names
        illegal_column_names = [
            c for c in type(self).return_columns if c not in
            legal_column_names
        ]
        if illegal_column_names: raise ValueError()

        super().__init__(*args, **kwargs)

    # TODO give a way of filtering or including Fields that are None...
    @property
    def subquery(self):
        return self.orm_entity.subquery_from_dict(
            *self.return_columns,
            **dict(self)
        )
