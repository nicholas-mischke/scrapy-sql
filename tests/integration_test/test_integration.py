
from pathlib import Path
import os
import subprocess
import sqlite3

cwd = Path.cwd()
# This file should be in same directory as scrapy.cfg
project_dir = Path(__file__).parent

if cwd != project_dir:
    os.chdir(project_dir)

# These are essentially temp files, that are used for outputs
# generated during a crawl. If they exists we'll delete them, as the crawl
# will generate new ones.
quotes_dot_db_file = project_dir / 'quotes.db'
quotes_dot_log_file = project_dir / 'quotes.log'
[f.unlink(missing_ok=True) for f in (quotes_dot_db_file, quotes_dot_log_file)]

subprocess.run('scrapy crawl quotes'.split(' '))

# Check the database file
conn = sqlite3.connect(quotes_dot_db_file)
cur = conn.cursor()

# The inserts happen in a random order...
# so knowing the ids of certain rows is important in later asserts


def test_records():

    # Testing author Table
    db_records = cur.execute(
        'SELECT name, birthday, bio FROM author'
    ).fetchall()

    # Get Einsteins ID
    einsteins_id = cur.execute(
        "SELECT id FROM author WHERE name = 'Albert Einstein'"
    ).fetchone()[0]
    kennedy_id = 1 if einsteins_id == 2 else 2

    correct_records = [
        (
            'Albert Einstein',
            '1879-03-14',
            'Won the 1921 Nobel Prize in Physics.'
        ),
        (
            'John F. Kennedy',
            '1917-05-29',
            '35th president of the United States.'
        )
    ]

    assert set(db_records) == set(correct_records)

    # Testing tag Table
    db_records = cur.execute('SELECT name FROM tag').fetchall()

    change_id = cur.execute(
        "SELECT id FROM tag WHERE name = 'change'"
    ).fetchone()[0]
    deep_thoughts_id = cur.execute(
        "SELECT id FROM tag WHERE name = 'deep-thoughts'"
    ).fetchone()[0]
    inspirational_id = cur.execute(
        "SELECT id FROM tag WHERE name = 'inspirational'"
    ).fetchone()[0]
    community_id = cur.execute(
        "SELECT id FROM tag WHERE name = 'community'"
    ).fetchone()[0]

    correct_records = [
        ('change', ), ('deep-thoughts', ),
        ('inspirational', ), ('community', )
    ]

    assert set(db_records) == set(correct_records)

    # Testing quote Table
    db_records = cur.execute('SELECT author_id, quote FROM quote').fetchall()

    einsteins_quote_I_id = cur.execute(
        "SELECT id FROM quote WHERE quote LIKE 'The world%'"
    ).fetchone()[0]
    einsteins_quote_II_id = cur.execute(
        "SELECT id FROM quote WHERE quote LIKE 'There are only%'"
    ).fetchone()[0]
    kennedy_I_id = cur.execute(
        "SELECT id FROM quote WHERE quote LIKE 'If not us, who%'"
    ).fetchone()[0]
    kennedy_II_id = cur.execute(
        "SELECT id FROM quote WHERE quote LIKE 'Ask not what%'"
    ).fetchone()[0]

    correct_records = [
        (
            einsteins_id,
            (
                'The world as we have created it is a process of our thinking. '
                'It cannot be changed without changing our thinking.'
            )
        ),
        (
            einsteins_id,
            (
                'There are only two ways to live your life. '
                'One is as though nothing is a miracle. '
                'The other is as though everything is a miracle.'
            )
        ),
        (
            kennedy_id, 'If not us, who? If not now, when?'
        ),
        (
            kennedy_id,
            (
                'Ask not what your country can do for you, '
                'but what you can do for your country.'
            )
        )
    ]

    assert set(db_records) == set(correct_records)

    # Testing quote_tag join Table
    db_records = cur.execute('SELECT * FROM quote_tag')
    db_records = db_records.fetchall()

    correct_records = [
        (einsteins_quote_I_id, change_id), (einsteins_quote_I_id, deep_thoughts_id),
        (kennedy_I_id, change_id), (kennedy_I_id, deep_thoughts_id),
        (einsteins_quote_II_id, inspirational_id), (kennedy_II_id, community_id)
    ]

    assert set(db_records) == set(correct_records)


def test_log_file():
    with quotes_dot_log_file.open() as f:
        content = f.read()
    assert 'log_count/ERROR' not in content


if cwd != project_dir:
    os.chdir(cwd)
