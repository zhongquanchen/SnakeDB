from template.tools.config import *
from template.model.page import *
from template.controller.disk import *
from template.controller.LRU import *


class buffer:
    def __init__(self):
        self.buffer_pool = []
        self.disk = disk(100)
        self.replace = LRU()

    def new_page(self):
        page = self.disk.allocate_page()
        Page.increase_pin()
        return page

    def fetch_page(self, page_id):
        for Pages in self.buffer_pool:
            if Pages.id == page_id:
                return Pages
        page = self.replace.evict()
        if Page.is_dirty():
            self.disk.write_page(page)

    # Close function
    def flush_page(self, page_id):
        for Pages in self.buffer_pool:
            if Pages.id == page_id:
                self.disk.write_page(Pages)
                return True
        return False

    def delete_page(self, page_id):
        for pages in self.buffer_pool:
            if pages.id == page_id:
                self.disk.delete_page(pages)

    def unpinning_page(self, page_id, is_dirty):
        for Pages in self.buffer_pool:
            if Pages.index == page_id:
                Pages.decrease_pin()
                if is_dirty:
                    Pages.dirty = True
                if Pages.pin_num == 0:
                    self.replace.use_append(Pages)
                return True
        return False


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
