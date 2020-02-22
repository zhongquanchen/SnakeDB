from template.controller.replace import *


class LRU(replace):
    def __init__(self):
        self.old = []

    def use_append(self, page):
        if page in self.old:
            self.old.remove(page)
        self.old.append(page)

    def access(self, page):
        if page in self.buffer:
            self.use_append(page)
            return
        if None in self.buffer:
            self.buffer[self.buffer.index(None)] = page
            self.use_append(page)
            return
        assert (len(self.old) > 0)
        is_replace = self.old.pop(0)
        self.buffer[self.buffer.index(is_replace)] = page
        self.use_append(page)
