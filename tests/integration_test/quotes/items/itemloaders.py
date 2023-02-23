
from quotes.items.models import Author, Tag, Quote
from quotes.items.processors import *

from itemloaders import ItemLoader
from itemloaders.processors import TakeFirst


class AuthorLoader(ItemLoader):
    default_item_class = Author
    default_input_processor = RemoveExcessWhitespace()
    default_output_processor = TakeFirst()

    # name_in = MapCompose(default_input_processor, TitleCase())
    birthday_in = StringToDate()


class TagLoader(ItemLoader):
    default_item_class = Tag
    default_input_processor = RemoveExcessWhitespace()
    default_output_processor = TakeFirst()


class QuoteLoader(ItemLoader):
    default_item_class = Quote
    default_input_processor = RemoveExcessWhitespace()
    default_output_processor = TakeFirst()

    # Passed a partially loaded Author instance
    # author_id_in = SubQueryPrimaryKey()
