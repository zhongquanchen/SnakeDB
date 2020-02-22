from template.tools.config import *
from template.model.page import *
from template.controller.disk import *
from template.controller.replace import *

class buffer:
    def __init__(self):
        self.buffer = []
        self.disk = disk()

    def fetch_page(self, page_id):
        for pages in self.buffer:
            if pages.id == page_id:
                return pages
        page = self.replace.evict()
        if page.is_dirty():
            self.disk.writePage(page)

    # Close function
    def flush_page(self, page_id):
        for pages in self.buffer:
            if pages.id == page_id:
                self.disk.writePage(pages)
                return True
        return False

    def delete_page(self, page_id):
        for pages in self.buffer:
            if pages.id == page_id:
                self.disk.deletePage(pages)


class BufferManager:

    def __init__(self):
        self.buffer = buffer()

    def insert(self):
        # a layer between query and buffer to implement insert
        pass

    def update(self):
        # a layer between query and buffer to implement update
        pass

    def select(self):
        # a layer between query and buffer to implement select
        pass