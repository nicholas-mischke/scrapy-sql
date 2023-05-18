
# Scrapy SQL

Scrapy SQL is currently in Beta.

This project integrates [SQLAlchemy](https://pypi.org/project/SQLAlchemy/) with [Scrapy](https://pypi.org/project/Scrapy/), providing an [itemadapter](https://pypi.org/project/itemadapter/) for SQLAlchemy models. It also introduces a subclass of the sqlalchemy.orm.Session class that enables bulk saves. This package employs the FeedExport extension, distinguishing itself from other Scrapy SQL integrations that use pipelines. Find a tutorial at the end of this README on using this package to scrape data from https://quotes.toscrape.com/ and save it in a SQLite database.

## Repositories
[PyPI](https://pypi.org/project/scrapy-sql/)

[GitHub](https://github.com/nicholas-mischke/scrapy-sql)

## Installation
ScrapySQL can be installed with pip.

```
$ pip install scrapy-sql
```

## Settings
Configure Scrapy SQL by using the settings below in your settings.py file.

Settings follow a priority hierarchy: feed_options['item_export_kwargs'] (if applicable), feed_options, and then crawler.settings.

```
# settings.py
# Below is the full list of possible settings that can be used to configure scrapy SQL.

# crawler.settings
SQLALCHEMY_DECLARATIVE_BASE = 'project.items.Base'  # No default value
SQLALCHEMY_ENGINE_ECHO = True  # Default: False 
SQLALCHEMY_SESSIONMAKER_KWARGS = {}  # Default: {'class_': 'scrapy_sql.session.ScrapyBulkSession'}
SQLALCHEMY_DEFAULT_ORM_STMT = 'scrapy_sql.stmts.insert_ignore'  # Default: 'scrapy_sql.stmts.insert'
SQLALCHEMY_ADD = 'path_to_custom_func'  # Default: session.add(instance)
SQLALCHEMY_COMMIT = 'path_to_custom_func'  # Default: session.commit()

# Allows for the FeedExport extension to be adjusted for use with scrapy-sql
FEED_STORAGES = {
    'mysql':    'scrapy_sql.feedexport.SQLAlchemyFeedStorage',
    'postgres': 'scrapy_sql.feedexport.SQLAlchemyFeedStorage',
    'sqlite':   'scrapy_sql.feedexport.SQLAlchemyFeedStorage'
}
FEED_EXPORTERS = {
    'sql': 'scrapy_sql.exporters.SQLAlchemyInstanceExporter'
}


# Any setting given in the feed_options dict overrides the corresponding global setting. The URI used as the 
# FEEDS key mirrors the connection string for SQLAlchemy.
# FEEDS is of format {'uri': feed_option_dict}
FEEDS = {
    'sqlite:///scrapy.db': {
        'format': 'sql',  # Format must be 'sql'
        'declarative_base': 'quotes.items.models.QuotesBase',  # Overrides SQLALCHEMY_DECLARATIVE_BASE
        'item_filter': 'scrapy_sql.feedexport.SQLAlchemyInstanceFilter',  # Optional, but recommended
        'engine_echo': True,  # Overrides SQLALCHEMY_ENGINE_ECHO
        'sessionmaker_kwargs': {},  # Overrides SQLALCHEMY_SESSIONMAKER_KWARGS
        'orm_stmts': {  # Overrides SQLALCHEMY_DEFAULT_ORM_STMT, allows different stmt for each model or table
            'quotes.items.models.Author': 'scrapy_sql.stmts.replace',
            'quotes.items.models.Tag': 'scrapy_sql.stmts.insert_ignore',
            'quotes.items.models.Quote': 'scrapy_sql.stmts.insert',
            'quotes.items.models.quote_tag_association_table': 'scrapy_sql.stmts.insert_ignore'
        },
        'commit': 'path_to_custom_func',  # Overrides SQLALCHEMY_COMMIT
        'add': 'path_to_custom_func',  # Overrides SQLALCHEMY_ADD
        'item_export_kwargs': {
            'add': 'path_to_custom_func'  # Overrides both SQLALCHEMY_ADD and 'add' in feed_options
        }
    }
}



```

## Opening an Issue

If you encounter a problem with the project or have a feature request, you can open an issue to let us know.

To open an issue, please follow these steps:

1. Go to the [Issues](https://github.com/nicholas-mischke/scrapy-sql/issues) tab on the github repository page.
2. Click on the "New Issue" button.
3. Provide a descriptive title for the issue.
4. In the issue description, provide detailed information about the problem you are experiencing or the feature you are requesting.
5. If applicable, include steps to reproduce the problem or any relevant code examples.
6. Add appropriate labels to categorize the issue (e.g., bug, enhancement, documentation).
7. Click on the "Submit new issue" button to create the issue.

Once you have opened an issue, our team will review it and provide assistance or discuss the requested feature.

Note: Before opening a new issue, please search the existing issues to see if a similar issue has already been reported. This helps avoid duplicates and allows us to focus on resolving existing problems.

## Contributing

Thank you for considering contributing to this project! We welcome your contributions to help make this project better.

To contribute to this project, please follow these steps:

1. Fork the repository by clicking on the "Fork" button at the top of the repository page. This will create a copy of the repository in your GitHub account.
2. Clone the forked repository to your local machine using Git:

    ```
    $ git clone https://github.com/your-username/scrapy-sql.git
    ```

3. Create a new branch for your changes:

    ```
    $ git checkout -b feature
    ```

4. Make your desired changes to the codebase.
5. Commit your changes with descriptive commit messages:

    ```
    $ git commit -m "Add new feature"
    ```

6. Push your changes to your forked repository:

    ```
    $ git push origin feature
    ```

7. Open a pull request (PR) from your forked repository to the original repository's `master` branch.
8. Provide a clear and descriptive title for your PR and explain the changes you have made.
9. Wait for the project maintainers to review your PR. You may need to make additional changes based on their feedback.
10. Once your PR is approved, it will be merged into the main codebase. Congratulations on your contribution!

If you have any questions or need further assistance, feel free to open an issue or reach out to the project maintainers.

Happy contributing!

## License
This project is licensed under the MIT License. See the LICENSE file for more details.

## Tutorial
Let's explore how this package functions by scraping the first page of https://quotes.toscrape.com/. We'll extract all quotes, authors, and tags and save them into a SQLite database.

Start by setting up the project.

We'll install scrapy & scrapy-sql as well as [scrapy processors](https://pypi.org/project/scrapy-processors/) , a package providing basic processors for item loaders. This tutorial won't cover its use, but if you're familiar with item loaders, you should find its application intuitive.
```
$ mkdir scrapy_tutorial
$ cd scrapy_tutorial
$ pipenv shell
$ pipenv install scrapy scrapy-processors scrapy-sql

```
With everything installed, we can initiate our project. The period at the end of the startproject command prevents Scrapy from creating a nested package structure.
```
$ scrapy startproject scrapy_tutorial . 
$ scrapy genspider quotes https://quotes.toscrape.com/
```

We won't need middlewares or pipelines for this tutorial, so let's remove them. We'll also add an itemloaders.py file. 
```
$ cd scrapy_tutorial
$ rm middlewares.py
$ rm pipelines.py
$ touch itemloaders.py
$ cd ..
```

Our project folder should now look like this:
```
$ tree
.
├── Pipfile
├── Pipfile.lock
├── scrapy.cfg
└── scrapy_tutorial
    ├── __init__.py
    ├── itemloaders.py
    ├── items.py
    ├── settings.py
    └── spiders
        ├── __init__.py
        └── quotes.py
```
Next, let's declare our models in items.py.

It's important to note the difference between declaring your Base class in Scrapy SQL vs vanilla SQLAlchemy.
Scrapy SQL uses dual inheritence with DeclarativeBase & ScrapyDeclarativeBase. ScrapyDeclarativeBase  provides many helper properties and methods used by ScrapyBulkSession for bulk inserts.

```
from sqlalchemy.orm import DeclarativeBase
from scrapy_sql import ScrapyDeclarativeBase

class Base(DeclarativeBase, ScrapyDeclarativeBase):
    pass

```

```
# items.py

from scrapy_sql import ScrapyDeclarativeBase

from sqlalchemy import Column, Date, ForeignKey, Integer, String, Table, Text
from sqlalchemy.orm import DeclarativeBase, relationship


class QuotesBase(DeclarativeBase, ScrapyDeclarativeBase):
    pass


class Author(Base):
    __tablename__ = 'author'

    id = Column(Integer, primary_key=True)

    name = Column(String(50), unique=True, nullable=False)
    birthday = Column(Date, nullable=False)
    bio = Column(Text, nullable=False)


class Tag(Base):
    __tablename__ = 'tag'

    id = Column(Integer, primary_key=True)
    name = Column(String(31), unique=True, nullable=False)


class Quote(Base):
    __tablename__ = 'quote'

    id = Column(Integer, primary_key=True)
    author_id = Column(ForeignKey('author.id'), nullable=False)

    quote = Column(Text, unique=True, nullable=False)

    author = relationship('Author')
    tags = relationship('Tag', secondary='quote_tag')


quote_tag_association_table = Table(
    'quote_tag', Base.metadata,
    Column('quote_id', ForeignKey('quote.id'), primary_key=True),
    Column('tag_id',   ForeignKey('tag.id'),   primary_key=True)
)
```

Next, we'll work on itemloaders.py.

```
# itemloaders.py

from scrapy_tutorial.items import Author, Tag, Quote

from itemloaders import ItemLoader
from itemloaders.processors import TakeFirst

from scrapy_processors import (
    EnsureEncoding,
    MapCompose,
    NormalizeWhitespace,
    StringToDate,
    StripQuotes,
)


clean_text = MapCompose(
    EnsureEncoding('utf-8'),
    NormalizeWhitespace(),
    StripQuotes()
)


class AuthorLoader(ItemLoader):
    default_item_class = Author
    
    default_input_processor = clean_text
    default_output_processor = TakeFirst()

    name_in = clean_text + str.title
    birthday_in = clean_text + StringToDate("%B %d, %Y")


class TagLoader(ItemLoader):
    default_item_class = Tag
    
    default_input_processor = clean_text
    default_output_processor = TakeFirst()

    name_in = clean_text + str.lower


class QuoteLoader(ItemLoader):
    default_item_class = Quote
    
    default_input_processor = clean_text
    default_output_processor = TakeFirst()
```

Now, let's edit settings.py to enable the FeedExport extension to work with SQLAlchemy.

We plan to save our data within the project folder (containing scrapy.cfg) to a file named scrapy_tutorial.db. Our log file differs only by extension (.log vs .db).

Note that the feed_export_option dictionary for our URI requires an additional key `declarative_base` with the value being the Base defined in our items.py file.

```
# settings.py

from pathlib import Path

LOG_FILE = Path(__file__).parent.parent / "scrapy_tutorial.log"

BOT_NAME = "scrapy_tutorial"

SPIDER_MODULES = ["scrapy_tutorial.spiders"]
NEWSPIDER_MODULE = "scrapy_tutorial.spiders"

FEED_STORAGES = {
    'sqlite': 'scrapy_sql.feedexport.SQLAlchemyFeedStorage'
}
FEED_EXPORTERS = {
    'sql': 'scrapy_sql.exporters.SQLAlchemyInstanceExporter'
}
FEEDS = {
    f'sqlite:///scrapy_tutorial.db': {
        'format': 'sql',
        'declarative_base': 'scrapy_tutorial.items.Base'
    }
}

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
```
Finally we'll create our spider. This spider has a number of shortcomings for now, that'll
be addressed shortly. 

```
# spiders/quotes.py

import scrapy
from scrapy_tutorial.itemloaders import AuthorLoader, TagLoader, QuoteLoader


class QuotesSpider(scrapy.Spider):
    name = 'quotes'
    start_urls = ['http://quotes.toscrape.com/']
    
    # parse_quotes
    def parse(self, response):
        # Scrapes only the first page of quotes
        
        for quote in response.xpath('//div[@class="quote"]'):
            quote_loader = QuoteLoader(selector=quote)
            quote_loader.add_xpath('quote', './/span[@class="text"]/text()')
            quote_instance = quote_loader.load_item()

            for tag in quote.xpath('.//a[@class="tag"]/text()').getall():
                tag_loader = TagLoader()
                tag_loader.add_value('name', tag)
                quote_instance.tags.append(tag_loader.load_item())

            yield quote_instance
            
            # Author container
            author_span = quote.xpath('.//span[contains(text(), "by")]')
            author_name = author_span.xpath('.//small[@class="author"]/text()').get()
            author_link = author_span.xpath('./a/@href').get()
            
            yield response.follow(author_link, callback=self.parse_author)
            
            
    def parse_author(self, response):
        loader = AuthorLoader(selector=response)

        loader.add_xpath('name', '//h3[@class="author-title"]/text()')
        loader.add_xpath('birthday', '//span[@class="author-born-date"]/text()')
        loader.add_xpath('bio', '//div[@class="author-description"]/text()')

        yield loader.load_item()
```
Even though we're expecting problems, let's run the spider and check the Log.
```
$ scrapy crawl quotes
```
Within the log file you'll find the following error. 
```
# scrapy_tutorial.log
sqlalchemy.exc.IntegrityError: (sqlite3.IntegrityError) UNIQUE constraint failed: tag.name
[SQL: INSERT INTO tag (name) VALUES (?)]
[parameters: [('change',), ('deep-thoughts',), ('thinking',), ('world',), ('abilities',), ('choices',), ('inspirational',), ('life',)  ... displaying 10 of 30 total bound parameter sets ...  ('obvious',), ('simile',)]]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
```

If you examine the scrapy_tutorial.db file, you'll observe that only the author table has been populated.

The author table was bulk INSERTed and the tag table was next (based on Base.metadata.sorted_tables),
but there are duplicate tag names. This is because each quote on the page is unique, but some quotes
share tags. The solution here is to use an INSERT OR IGNORE statement. This way, duplicate tags won't lead to the abortion of the rest of the INSERTs.

To achieve this, we need to return to settings.py and modify it. Note the addition of an orm_stmts key to the feed_options dict in the edited code. By default, when the ScrapyBulkSession carries out bulk inserts, it uses a straightforward INSERT statement. However, by modifying this key, we can change the insertion of Tags to INSERT OR IGNORE.


```
# settings.py


from pathlib import Path

LOG_FILE = Path(__file__).parent.parent / "scrapy_tutorial.log"

BOT_NAME = "scrapy_tutorial"

SPIDER_MODULES = ["scrapy_tutorial.spiders"]
NEWSPIDER_MODULE = "scrapy_tutorial.spiders"

FEED_STORAGES = {
    'sqlite': 'scrapy_sql.feedexport.SQLAlchemyFeedStorage'
}
FEED_EXPORTERS = {
    'sql': 'scrapy_sql.exporters.SQLAlchemyInstanceExporter'
}
FEEDS = {
    f'sqlite:///scrapy_tutorial.db': {
        'format': 'sql',
        'declarative_base': 'scrapy_tutorial.items.Base',
        # new
        'orm_stmts': {
            'scrapy_tutorial.items.Tag': 'scrapy_sql.stmts.insert_ignore'
        }
    }
}

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
```
Let's save our new settings.py file, delete the .log and .db file and crawl again to see the results.

```
$ scrapy crawl quotes
```

If we now review our scrapy_tutorial.db file we'll see that we've populated both the author and tag tables, but not the quote or quote_tag table. In the scrapy_tutorial.log, the error message indicates a problem with the author_id of the quote:

```
# scrapy_tutorial.log

sqlalchemy.exc.IntegrityError: (sqlite3.IntegrityError) NOT NULL constraint failed: quote.author_id
[SQL: INSERT INTO quote (quote) VALUES (?)]
[parameters: [('The world as we have created it is a process of our thinking. It cannot be changed without changing our thinking.',), ('It is our choices, Harry, that show what we truly are, far more than our abilities.',), ('There are only two ways to live your life. One is as though nothing is a miracle. The other is as though everything is a miracle.',), ('The person, be it gentleman or lady, who has not pleasure in a good novel, must be intolerably stupid.',), ("Imperfection is beauty, madness is genius and it's better to be absolutely ridiculous than absolutely boring.",), ('Try not to become a man of success. Rather become a man of value.',), ('It is better to be hated for what you are than to be loved for what you are not.',), ("I have not failed. I've just found 10,000 ways that won't work.",), ("A woman is like a tea bag; you never know how strong it is until it's in hot water.",), ('A day without sunshine is like, you know, night.',)]]
(Background on this error at: https://sqlalche.me/e/20/gkpj)

```

This error arises because there isn't a correlation between the quote and the author in the current spider configuration. There are two ways to solve this:

Passing the quote_instance in the meta tag of the response.follow, or
Loading the quote_instance.author_id attribute with a subquery that obtains the author_id from the author's name during the INSERT operation.
Given that the first method would necessitate setting up an HTTP cache to avoid re-crawling the same pages due to Scrapy's DupeFilter, the second option might be preferable in this case. This is where scrapy_sql.subquery_item.SubqueryItem comes into play, allowing us to generate a subquery easily.

Let's start by declaring the SubqueryItem in items.py. Here's how you could define it:

```
# items.py
from scrapy_sql import ScrapyDeclarativeBase
from scrapy_sql.subquery_item import SubqueryItem, Field # new

from sqlalchemy import Column, Date, ForeignKey, Integer, String, Table, Text
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase, ScrapyDeclarativeBase):
    pass


class Author(Base):
    __tablename__ = 'author'

    id = Column(Integer, primary_key=True)

    name = Column(String(50), unique=True, nullable=False)
    birthday = Column(Date, nullable=False)
    bio = Column(Text, nullable=False)


class Tag(Base):
    __tablename__ = 'tag'

    id = Column(Integer, primary_key=True)
    name = Column(String(31), unique=True, nullable=False)


class Quote(Base):
    __tablename__ = 'quote'

    id = Column(Integer, primary_key=True)
    author_id = Column(ForeignKey('author.id'), nullable=False)

    quote = Column(Text, unique=True, nullable=False)

    author = relationship('Author')
    tags = relationship('Tag', secondary='quote_tag')


quote_tag_association_table = Table(
    'quote_tag', Base.metadata,
    Column('quote_id', ForeignKey('quote.id'), primary_key=True),
    Column('tag_id',   ForeignKey('tag.id'),   primary_key=True)
)

# new
class AuthorSubqueryItem(SubqueryItem):
    orm_entity = Author
    return_columns = ('id', ) # defaults to primary_key, but explicit is better than implicit
    
    name = Field()

    # The subquery property of an instance of this class will be a subquery with string representation of
    # SELECT author.id FROM author WHERE author.name = "Albert Einstein" 

```

Let's change our itemloaders.py file next. You'll see that the SubQueryItem can be
loaded and use processors just like any other scrapy Item
```
# itemloaders.py

from scrapy_tutorial.items import Author, Tag, Quote, AuthorSubqueryItem # new

from itemloaders import ItemLoader
from itemloaders.processors import TakeFirst

from scrapy_processors import (
    EnsureEncoding,
    MapCompose,
    NormalizeWhitespace,
    StringToDate,
    StripQuotes,
)


clean_text = MapCompose(
    EnsureEncoding('utf-8'),
    NormalizeWhitespace(),
    StripQuotes()
)


class AuthorLoader(ItemLoader):
    default_item_class = Author
    
    default_input_processor = clean_text
    default_output_processor = TakeFirst()

    name_in = clean_text + str.title
    birthday_in = clean_text + StringToDate("%B %d, %Y")


class TagLoader(ItemLoader):
    default_item_class = Tag
    
    default_input_processor = clean_text
    default_output_processor = TakeFirst()

    name_in = clean_text + str.lower


class QuoteLoader(ItemLoader):
    default_item_class = Quote
    
    default_input_processor = clean_text
    default_output_processor = TakeFirst()

# new
class AuthorSubqueryLoader(ItemLoader):
    default_item_class = AuthorSubqueryItem
    
    default_input_processor = clean_text + str.title
    default_output_processor = TakeFirst()

```

Finally, let's modify our spider.
```
# spiders/quotes.py

import scrapy
from scrapy_tutorial.itemloaders import AuthorLoader, TagLoader, QuoteLoader, AuthorSubqueryLoader # new


class QuotesSpider(scrapy.Spider):
    name = 'quotes'
    start_urls = ['http://quotes.toscrape.com/']
    
    # parse_quotes
    def parse(self, response):
        # Scrapes only the first page of quotes, for now... 
        
        for quote in response.xpath('//div[@class="quote"]'):
            quote_loader = QuoteLoader(selector=quote)
            quote_loader.add_xpath('quote', './/span[@class="text"]/text()')
            quote_instance = quote_loader.load_item()
            
            for tag in quote.xpath('.//a[@class="tag"]/text()').getall():
                tag_loader = TagLoader()
                tag_loader.add_value('name', tag)
                quote_instance.tags.append(tag_loader.load_item())
            
            author_span = quote.xpath('.//span[contains(text(), "by")]')
            author_name = author_span.xpath('.//small[@class="author"]/text()').get()
            author_link = author_span.xpath('./a/@href').get()
            
            # new
            author_loader = AuthorSubqueryLoader()
            author_loader.add_value('name', author_name)
            # constructs a subquery for the author_id from from author's name
            # Then assign the result to the quote instance
            author_subquery = author_loader.load_item().subquery
            quote_instance.author_id = author_subquery
            
            yield quote_instance
            yield response.follow(author_link, callback=self.parse_author)
            
            
    def parse_author(self, response):
        loader = AuthorLoader(selector=response)

        loader.add_xpath('name', '//h3[@class="author-title"]/text()')
        loader.add_xpath('birthday', '//span[@class="author-born-date"]/text()')
        loader.add_xpath('bio', '//div[@class="author-description"]/text()')

        yield loader.load_item()
```

Let's crawl again, this time we shouldn't see any errors.
```
$ scrapy crawl quotes
```

Checking the log file, we see no errors. Checking the SQLite file all tables, including the association table between quotes and tags, are fully populated. Subqueries allowed you to perform these operations without direct read operations from the database.

This ability to write to your database efficiently, using subqueries and avoiding unnecessary reads, is one of the significant advantages of using Scrapy with SQLAlchemy and the Scrapy SQL library.

The logging file from Scrapy also provides you with detailed information about the operations performed during the spider run, which is extremely useful for debugging and optimization.

Keep in mind that for more complex scrapers, you might encounter other challenges related to handling relationships, dealing with duplicated entries, or managing more complex data types. Always consider the specific requirements of your project when designing your Scrapy spiders and the corresponding data models.

At this point, you have successfully created a Scrapy project that scrapes data from a website and stores it in a SQLite database using SQLAlchemy models. You have also encountered and solved some common problems related to handling relationships and unique constraints in SQL databases. This gives you a good foundation for developing more complex web scraping projects in the future. Congratulations!