from template.tools.config import *
from template.model.page import *
from template.controller.disk import *
from template.controller.replace import *

class Buffer:
    def __init__(self):
        # buffer pages
        self.bufferpool = {}
        print("initialized")
#        self.disk = disk()

    def fetch_page(self, page_id):
        for pages in self.bufferpool:
            if pages.id == page_id:
                return pages
        page = self.replace.evict()
        if page.is_dirty():
            self.disk.writePage(page)

    # Close function
    def flush_page(self, page_id):
        for pages in self.bufferpool:
            if pages.id == page_id:
                self.disk.writePage(pages)
                return True
        return False

    def delete_page(self, page_id):
        for pages in self.bufferpool:
            if pages.id == page_id:
                self.disk.deletePage(pages)


class BufferManager:

    def __init__(self):
        self.buffer = Buffer()
        self.cur_counter = 0

    def insert(self):
        # a layer between query and buffer to implement insert
        pass

    def update(self):
        # a layer between query and buffer to implement update
        pass

    def select(self):
        # a layer between query and buffer to implement select
        pass

    def loadPages(self):
        # return a corresponding pages
        pass

    def update(self, pages):
        self.cur_counter += 1
        pages_id = RANDOM_ID + self.cur_counter
        self.buffer.bufferpool.update({pages_id:pages})
        return pages_id

    def get_pages(self, pages_id):
        pages = self.buffer.bufferpool[pages_id]
        return pages
