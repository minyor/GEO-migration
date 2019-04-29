

class Contractor:
    def __init__(self):
        self.nodes = dict()


class Context:
    def __init__(self):
        self.address = None
        self.observers = "127.0.0.1:4000,127.0.0.1:4001,127.0.0.1:4002"
        self.verbose = False
        self.nodes = dict()
        self.contractors = dict()
        self.channels = dict()
