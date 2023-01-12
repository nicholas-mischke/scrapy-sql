
from pathlib import Path
import os
import subprocess
import sqlite3
from pprint import pprint

cwd = Path.cwd()

curr_file = Path(__file__)
scrapy_integration_test_project_dir = curr_file.parent / 'integration_test'
os.chdir(scrapy_integration_test_project_dir)

# These are essentially temp files, that are used for outputs
# used during a crawl. If they exists we'll delete them, as the crawl
# will generate new ones.
quotes_dot_db_file = scrapy_integration_test_project_dir / 'quotes.db'
quotes_dot_log_file = scrapy_integration_test_project_dir / 'quotes.log'
[f.unlink(missing_ok=True) for f in (quotes_dot_db_file, quotes_dot_log_file)]

subprocess.run('scrapy crawl quotes'.split(' '))

# Check the database file
conn = sqlite3.connect(quotes_dot_db_file)
cur = conn.cursor()

# The inserts happen in a random order...
# so remove the id column from each table
def test_author_records():
    author_records = cur.execute('SELECT name, birthday, bio FROM author')
    author_records = author_records.fetchall()

    correct_records = [
        (
         'Albert Einstein',
         '1879-03-14 00:00:00.000000',
         'Won the 1921 Nobel Prize in Physics for his paper explaining the '
         'photoelectric effect.'
        ),
        (
         'John F. Kennedy',
         '1917-05-29 00:00:00.000000',
         'American politician who served as the 35th president of the United States.'
        )
    ]

    assert set(author_records) == set(correct_records)


def test_quote_records():
    quote_records = cur.execute('SELECT quote, author FROM quote')
    quote_records = quote_records.fetchall()

    correct_records = [
        (
         'The world as we have created it is a process of our thinking. It cannot be '
         'changed without changing our thinking.',
         'Albert Einstein'
        ),
        (
         'There are only two ways to live your life. One is as though nothing is a '
         'miracle. The other is as though everything is a miracle.',
         'Albert Einstein'
        ),
        (
         'Ask not what your country can do for you, but what you can do for your '
         'country.',
         'John F. Kennedy'
        ),
        ('If not us, who? If not now, when?', 'John F. Kennedy')
    ]

    assert set(quote_records) == set(correct_records)


def test_quote_tag_records():
    quote_tag_records = cur.execute('SELECT * FROM quote_tag')
    quote_tag_records = quote_tag_records.fetchall()

    assert len(quote_tag_records) == 6


def test_tag_records():
    tag_records = cur.execute('SELECT name FROM tag')
    tag_records = tag_records.fetchall()

    correct_records = [
        ('change', ), ('deep-thoughts', ),
        ('inspirational', ), ('community', )
    ]

    assert set(tag_records) == set(correct_records)


def test_log_file():
    with quotes_dot_log_file.open() as f:
        content = f.read()
    assert 'log_count/ERROR' not in content


os.chdir(cwd)
# [f.unlink(missing_ok=True) for f in (quotes_dot_db_file, quotes_dot_log_file)]
