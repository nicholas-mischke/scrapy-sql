
from scrapy.exporters import BaseItemExporter
from scrapy.utils.misc import load_object


def _default_add(session, table):
    session.add(table)


class SQLAlchemyTableExporter(BaseItemExporter):

    def __init__(self, session, **kwargs):
        self.session = session
        self.add = load_object(
            kwargs.get('sqlalchemy_add')
            or _default_add
        )

    def export_item(self, table):
        self.add(self.session, table)
