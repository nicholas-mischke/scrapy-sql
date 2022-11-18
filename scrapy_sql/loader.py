
from scrapy.loader import ItemLoader

from sqlalchemy import func
from sqlalchemy.types import (
    DATE, Date,
    DATETIME, DateTime,
    TIME, Time, TIMESTAMP
)

import sessions


date_types = (DATE, Date, DATETIME, DateTime, TIME, Time, TIMESTAMP)


class TableLoader(ItemLoader):

    def load_table(self):
        """
        If a SQLAlchemy table object is already in the project's session
        return the obj already in session. This prevents duplicates from being
        added to the session.
        """
        table = self.load_item()

        # Check to be sure no lists are involved in columns attrs
        # This happens with ItemLoaders / TableLoader objs don't properly
        # utilize an output processor to remove scraped data from the lists
        # they're appended to in the ItemLoader / TableLoader
        any_lists = any([isinstance(x, list) for x in table.asdict().values()])

        # Autoincrement columns get initialized as None, but get assigned
        # a value once added to the session (even if not yet in the DB)
        # This dict comprehension removes these NoneTypes to avoid a mis-filtering.
        filter_kwargs = {k: v for k, v in table.asdict().items()
                         if v is not None}

        # SQLite doesn't have built in DATE types.
        # This for loop avoids a mis-filtering on DATE types in SQLite.
        # https://gist.github.com/danielthiel/8374607
        for c in table.columns:
            if isinstance(c.type, date_types):
                del filter_kwargs[c.name]
                # filter_kwargs[c.name] = func.DATE(c)

        exists = sessions.session.query(
            table.__class__
        ).filter_by(
            **filter_kwargs
        ).first()

        return exists if exists is not None else table
