
from scrapy import Item, Field


class SubqueryItem(Item):

    orm_entity = None
    return_columns = tuple()

    @property
    def subquery(self):
        return type(self).orm_entity.subquery_from_dict(
            dict(self),
            *type(self).return_columns
        )