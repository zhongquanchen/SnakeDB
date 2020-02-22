from template.tools.config import *
from template.model.page import *
from template.controller.disk import *
from template.controller.LRU import *


class Buffer:
    def __init__(self):
        # buffer pages
        self.bufferpool = {}
        self.disk = disk(100)
        self.replace = LRU()

    # search for the correspond pages in buffer, if not then search in disk
    def fetch_page(self, page_id):
        if page_id in self.bufferpool:
            return self.bufferpool[page_id]

        # find page in disk
        page = self.disk.readPage(page_id)

        # for Pages in self.buffer_pool:
        #     if Pages.id == page_id:
        #         return Pages
        # page = self.replace.evict()
        # if Page.is_dirty():
        #     self.disk.write_page(page)

    # Close function
    def flush_page(self, page_id):
        for Pages in self.buffer_pool:
            if Pages.id == page_id:
                self.disk.write_page(Pages)
                return True
        return False

    def delete_page(self, page_id):
        for pages in self.bufferpool:
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

    def bufferpool_capacity(self):
        if len(self.bufferpool)+SPACE_LEFT > BUFFER_SIZE:
            return False
        return True


class BufferManager:

    def __init__(self):
        self.buffer = Buffer()
        self.cur_counter = 0

    def update(self, pages_id, pages):
        self.buffer.bufferpool.update({pages_id:pages})

    def get_pages(self, pages_id):
        pages = self.buffer.bufferpool[pages_id]
        return pages


