
from scrapy.extensions.feedexport import BlockingFeedStorage, build_storage
import sessions


def _store(store_method, session):
    if store_method:
        store_method(session)
    else:
        session.commit()
        session.close()


class SQLAlchemyFeedStorage(BlockingFeedStorage):

    @classmethod
    def from_crawler(cls, crawler, uri, *, feed_options=None):
        return build_storage(
            cls,
            uri,
            store_method=crawler.settings['SQLALCHEMY_STORE'] or None,
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
        _store(self.store_method, session)


# SQLite is not thread safe
class SQLAlchemySQLiteFeedStorage(SQLAlchemyFeedStorage):

    def store(self, session):
        _store(self.store_method, session)
