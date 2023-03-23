
import pytest

from integration_test.quotes.items.models import (
    QuotesBase, Author, Tag, Quote, t_quote_tag
)

from scrapy_sql.utils import subquery_to_string
from scrapy_sql.subquery_item import SubqueryItem, Field


class AuthorSubqueryItem_I(SubqueryItem):

    orm_entity = Author
    return_columns = tuple() # test to see if returns `id`, since it's the pk

    name = Field()


class AuthorSubqueryItem_II(SubqueryItem):

    orm_entity = Author
    return_columns = ('name', 'bio')

    id = Field()


subquery_item_I = AuthorSubqueryItem_I()
subquery_item_I['name'] = 'Fat Albert'

subquery_item_II = AuthorSubqueryItem_II()
subquery_item_II['id'] = 1


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            subquery_item_I,
            'SELECT author.id FROM author WHERE author.name = "Fat Albert"'
        ),
        (
            subquery_item_II,
            'SELECT author.name, author.bio FROM author WHERE author.id = 1'
        )
    ]
)
def test_suquery(input, expected):
    assert subquery_to_string(input.subquery) == expected


