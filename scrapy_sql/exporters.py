
from scrapy.exporters import BaseItemExporter
from scrapy.utils.misc import load_object


class SQLAlchemyInstanceExporter(BaseItemExporter):

    # **kwargs is item_export_kwargs sat in feed_options
    def __init__(self, session, **kwargs):
        self.session = session
        self.add = load_object(kwargs['add'])

    def export_item(self, instance):
        self.add(self.session, instance)
