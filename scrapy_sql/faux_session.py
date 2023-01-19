


class FauxSession:

    def __init__(self):
        self.tables = set()

    def add(self, table):
        pass

    def __contains__(self, table):
        return table in self.tables
