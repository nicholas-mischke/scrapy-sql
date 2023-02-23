
from quotes.models import Author, Tag, Quote
from quotes.processors import *

from itemloaders import ItemLoader
from itemloaders.processors import TakeFirst


class AuthorLoader(ItemLoader):
    default_item_class = Author
    default_input_processor = CleanText()
    default_output_processor = TakeFirst()

    name_in = CleanText(title=True)
    birthday_in = StringToDate()


class TagLoader(ItemLoader):
    default_item_class = Tag
    default_input_processor = CleanText(lower=True)
    default_output_processor = TakeFirst()


class QuoteLoader(ItemLoader):
    default_item_class = Quote
    default_input_processor = CleanText()
    default_output_processor = TakeFirst()

    author_id_in = MapCompose(
        CleanText(),
        QueryAuthor()
    )
