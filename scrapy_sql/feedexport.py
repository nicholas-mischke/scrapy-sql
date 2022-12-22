
from scrapy.extensions.feedexport import BlockingFeedStorage, build_storage
from scrapy.utils.misc import load_object
import scrapy_sql.sessions as sessions


def _commit(sqlalchemy_commit, session):
    if sqlalchemy_commit is not None:
        sqlalchemy_commit(session)
    else:
        session.commit()
        session.close()


class SQLAlchemyFeedStorage(BlockingFeedStorage):

    @classmethod
    def from_crawler(cls, crawler, uri, *, feed_options=None):

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
        self.feed_options = feed_options
        self.store_method = store_method

    def open(self, spider):
        """
        Typically opens a file, or file like object and returns it.
        This obj is passed to the constructor of the ItemExporter class
        """
        # The session returned by this method is the crawl's active session
        # meaning it's used for exporting & via loader.TableLoader.load_table method
        active_session = sessions.create_new_session()
        sessions.session = active_session
        return active_session

    def _store_in_thread(self, session):
        """
        commits all items held in the session to the database
        then closes the session.

        Called from store(self, session) of parent class
        """
        _commit(self.store_method, session)


# SQLite is not thread safe
class SQLAlchemySQLiteFeedStorage(SQLAlchemyFeedStorage):

    def store(self, session):
        _commit(self.store_method, session)
