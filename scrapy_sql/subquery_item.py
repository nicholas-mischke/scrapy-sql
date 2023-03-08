
from scrapy import Item, Field


class SubqueryItem(Item):
    """
    Used alongside ScrapyDeclarativeBase subclasses for orm_entities
    this class inherits from scrapy.Item and turns the dict representation
    into a SQLAlchemy subquery
    """

    orm_entity = None
    return_columns = tuple()

    @property
    def subquery(self):
        return type(self).orm_entity.subquery_from_dict(
            dict(self),
            *type(self).return_columns
        )