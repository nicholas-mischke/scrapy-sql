
from scrapy.exporters import BaseItemExporter
from scrapy.utils.misc import load_object


class SQLAlchemyTableExporter(BaseItemExporter):
    def __init__(self, session, **kwargs):
        """
        session is a sqlalchemy.orm session obj.
        For most ItemExporters this is a file, or file-like object.
        Really, just some place where the items can be persisted into storage.
        the session obj is returned from the FeedStorage open method

        These values are stat in the option_dict of
        settings.FEEDS = {uri:option_dict}
        kwargs = {
            'fields_to_export': None,
            'encoding': 'utf8',
            'indent': 4
        } plus anything in `option_dict['item_export_kwargs']`

        The default key, value pairs in kwargs are used by the parent class
        in it's _configure method. They serve no use in this class.
        """
        super().__init__(
            dont_fail=kwargs.get('dont_fail', True),
            **kwargs
        )
        self.session = session
        self.add = kwargs.get('sqlalchemy_add', None)

    def export_item(self, table):
        if self.add is not None:
            self.add(self.session, table)
        else:
            if table not in self.session:
                self.session.add(table)
