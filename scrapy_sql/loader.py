
from sqlalchemy import func, Integer
from sqlalchemy.types import (
    DATE, Date,
    DATETIME, DateTime,
    TIME, Time,
    TIMESTAMP
)
from scrapy.loader import ItemLoader
import scrapy_sql


class TableLoader(ItemLoader):

    def load_table(self):

        table = self.load_item()
        filter_kwargs = table.asdict()

        # Autoincrement columns have a value of `None` before persistence.
        # For this reason we'll disregard autoincrementing columns when
        # determining if two table objects are equal.

        # SQLAlchemy will automatically set the first Integer PK column
        # that's not marked as a FK as autoincrement = True
        for column in table.columns:
            if (
                column.autoincrement is True
                or (
                    isinstance(column.type, Integer)
                    and column.primary_key is True
                    and column.foreign_keys == set()
                )
                and filter_kwargs[column.name] is None
            ):
                del filter_kwargs[column.name]

        # SQLite doesn't have built in DATE/TIME types (they're converted to strings).
        # This for loop avoids a mis-filtering on these types in SQLite.
        # https://gist.github.com/danielthiel/8374607
        for column in table.columns:
            if isinstance(column.type, (DATE, Date)):
                filter_kwargs[column.name] = func.DATE(column)
            elif isinstance(column.type, (DATETIME, DateTime)):
                filter_kwargs[column.name] = func.DATETIME(column)
            elif isinstance(column.type, (TIME, Time)):
                filter_kwargs[column.name] = func.TIME(column)
            elif isinstance(column.type, TIMESTAMP):
                filter_kwargs[column.name] = func.TIMESTAMP(column)

        exists = scrapy_sql.sessions.session.query(
            table.__class__
        ).filter_by(
            **filter_kwargs
        ).first()

        return exists if exists is not None else table