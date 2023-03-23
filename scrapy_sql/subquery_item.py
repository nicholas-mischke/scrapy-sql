
from scrapy import Item, Field


class SubqueryItem(Item):
    """
    Used alongside ScrapyDeclarativeBase subclasses for orm_entities
    this class inherits from scrapy.Item and turns the dict representation
    into a SQLAlchemy subquery
    """

    orm_entity = None
    return_columns = tuple()

    # TODO raise exception if SubqueryItem Fields aren't a subset
    # of orm_entity columns / relationships.
    # TODO give a way of filtering or including Fields that are None...
    @property
    def subquery(self):
        return self.orm_entity.subquery_from_dict(
            *self.return_columns,
            **dict(self)
        )