
import pytest

import _test_feedexport_helpers
from _test_feedexport_helpers import LoadTable_Model, load_table_table

from integration_test_project.quotes.items.models import (
    QuotesBase, Author, Tag, Quote, t_quote_tag
)

from scrapy_sql.feedexport import *

from scrapy.crawler import Crawler
from scrapy.spiders import Spider

from sqlalchemy import Engine


class TestSQLAlchemyInstanceFilter:

    def test_dunder_init(self):
        feed_options = {'declarative_base': QuotesBase}
        filter = SQLAlchemyInstanceFilter(feed_options)

        assert filter.feed_options == feed_options
        assert filter.Base == QuotesBase
        assert filter.instance_classes == (Author, Tag, Quote)

    @pytest.mark.parametrize(
        'input, expected',
        [
            (Author(), True),
            (Tag(), True),
            (Quote(), True),
            (t_quote_tag, False),
            ({}, False)
        ]
    )
    def test_accepts(self, input, expected):
        feed_options = {'declarative_base': QuotesBase}
        filter = SQLAlchemyInstanceFilter(feed_options)

        assert filter.accepts(input) == expected


class TestSQLAlchemyFeedStorage:

    @pytest.mark.parametrize(
        "settings_dict, input_feed_options, expected_feed_options, sessionmaker_kwargs_contains_feed_options",
        [
            # Verify defaults
            (
                _test_feedexport_helpers.default_settings_dict,
                _test_feedexport_helpers.default_input_feed_options,
                _test_feedexport_helpers.default_expected_feed_options,
                True
            ),
            # Verify globals override defaults
            (
                _test_feedexport_helpers.global_settings_dict,
                _test_feedexport_helpers.global_input_feed_options,
                _test_feedexport_helpers.global_expected_feed_options,
                False
            ),
            # Verify feed_options overrides globals & defaults
            (
                _test_feedexport_helpers.feed_options_settings_dict,
                _test_feedexport_helpers.feed_options_input_feed_options,
                _test_feedexport_helpers.feed_options_expected_feed_options,
                False
            ),
            # Verify item_export_kwargs override feed_options, globals & defaults
            (
                _test_feedexport_helpers.item_export_kwargs_settings_dict,
                _test_feedexport_helpers.item_export_kwargs_input_feed_options,
                _test_feedexport_helpers.item_export_kwargs_expected_feed_options,
                False
            )
        ]
    )
    def test_from_crawler(self, settings_dict, input_feed_options, expected_feed_options, sessionmaker_kwargs_contains_feed_options):

        feed_options = SQLAlchemyFeedStorage.from_crawler(
            crawler=Crawler(Spider, settings_dict),
            uri='sqlite:///memory:',
            feed_options=input_feed_options
        ).feed_options

        # Easier to delete the specific engine instance that's created
        sessionmaker_kwargs = feed_options.get('sessionmaker_kwargs')

        assert isinstance(sessionmaker_kwargs['bind'], Engine)
        del sessionmaker_kwargs['bind']

        if sessionmaker_kwargs_contains_feed_options:
            assert sessionmaker_kwargs['feed_options'] == feed_options
            del sessionmaker_kwargs['feed_options']
        else:
            with pytest.raises(KeyError):
                sessionmaker_kwargs['feed_options']

        feed_options['sessionmaker_kwargs'] = sessionmaker_kwargs

        #
        assert feed_options == expected_feed_options

    @pytest.mark.skip(reason="wrapper func")
    def test_open(self):
        pass

    @pytest.mark.skip(reason="wrapper func")
    def test_store(self):
        pass

    @pytest.mark.skip(reason="wrapper func")
    def test_close_spider(self):
        pass

    @pytest.mark.parametrize(
        "obj, table",
        [
            (
                '_test_feedexport_helpers.LoadTable_Model',
                LoadTable_Model.__table__
            ),  # String Model
            (
                '_test_feedexport_helpers.load_table_table',
                load_table_table
            ),  # String table
            (
                LoadTable_Model,
                LoadTable_Model.__table__
            ), # Model
            (
                load_table_table,
                load_table_table
            ), # Table
        ]
    )
    def test_under_load_table(self, obj, table):
       assert SQLAlchemyFeedStorage._load_table(obj) == table
