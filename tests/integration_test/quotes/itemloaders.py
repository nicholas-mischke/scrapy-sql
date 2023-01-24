
from quotes.models import *
from quotes.processors import *

from itemloaders import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose


class QuoteLoader(ItemLoader):
    default_item_class = Quote
    default_input_processor = RemoveExcessWhiteSpaces()
    default_output_processor = TakeFirst()

    authorDOTname = MapCompose(
        RemoveExcessWhiteSpaces
    )
    tags_out = MapCompose(
        TakeAll
    )


class AuthorLoader(ItemLoader):
    default_item_class = Author
    default_input_processor = RemoveExcessWhiteSpaces()
    default_output_processor = TakeFirst()

    birthday_in = MapCompose(to_datetime_obj)


class TagLoader(ItemLoader):
    default_item_class = Tag
    default_input_processor = RemoveExcessWhiteSpaces()
    default_output_processor = TakeFirst()
