
from scrapy.extensions.feedexport import BlockingFeedStorage, build_storage
from scrapy.utils.misc import load_object

import scrapy_sql
from scrapy_sql.sessions import create_new_session


def _default_store_method(session):
    session.commit()
    session.close()


class SQLAlchemyFeedStorage(BlockingFeedStorage):

    @classmethod
    def from_crawler(cls, crawler, uri, *, feed_options=None):

        feed_options.setdefault('item_export_kwargs', {})

        add = (
            feed_options.get('item_export_kwargs').get('sqlalchemy_add', None)
            or feed_options.get('sqlalchemy_add', None)
            or crawler.settings.get('SQLALCHEMY_ADD', None)
        )
        if add:
            add = load_object(add)
        # Update the kwargs passed to the ItemExporter obj
        # to have the callable obj for adding tables to the session.
        feed_options.get('item_export_kwargs').setdefault(
            'sqlalchemy_add',
            add
        )

        commit = (
            feed_options.get('sqlalchemy_commit', None)
            or crawler.settings.get('SQLALCHEMY_COMMIT', None)
        )
        if commit:
            commit = load_object(commit)

        return build_storage(
            cls,
            uri,
            store_method=commit,
            feed_options=feed_options,
        )

    def __init__(self, uri, *, store_method=None, feed_options=None):
        self.uri = uri
        self.store_method = store_method
        self.feed_options = feed_options

    def open(self, spider):
        """
        Typically opens a file, or file like object and returns it.
        This obj is passed to the constructor of the ItemExporter class
        """
        session = create_new_session()
        scrapy_sql.sessions.session = session
        return session

    def _store_in_thread(self, session):
        """
        commits all items held in the session to the database
        then closes the session.

        Called from store(self, session) of parent class
        """
        if self.store_method is not None:
            self.store_method(session)
        else:
            _default_store_method(session)


# SQLite is not thread safe
class SQLAlchemySQLiteFeedStorage(SQLAlchemyFeedStorage):

    def store(self, session):
        super()._store_in_thread(session)
