
class Obj:

    def __init__(self):
        self.container = []

    def append(self, data):
        self.container.append(data)

    def __str__(self):
        return str(self.container)

obj = Obj()
