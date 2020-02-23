class replace(object):
    def __init__(self, size_buffer):
        self.size_buffer = size_buffer
        self.buffer = [None] * size_buffer

    def insert(self):
        pass

    def evict(self):
        pass

    def access(self, page):
        raise NotImplementedError

    def get_buffer(self):
        return self.buffer
