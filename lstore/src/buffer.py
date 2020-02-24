from lstore.src.config import *
from lstore.src.page import *
from lstore.src.disk import *


class Buffer:
    def __init__(self):
        # buffer pages
        self.bufferpool = {}
        self.buffersize = BUFFER_SIZE
        self.cur_size = 0
        self.disk = disk(100)
        self.replace = LRU()

    # Close function
    def flush_page(self, pages_len, table_name):
        old_data = self.replace.pop_top()
        if old_data in self.bufferpool:
            self.disk.writePage(self.bufferpool[old_data], table_name)
            del self.bufferpool[old_data]
            self.cur_size -= pages_len

    def delete_page(self, page_id, table_name):
        for pages in self.bufferpool:
            if pages.id == page_id:
                self.disk.delete_page(pages, table_name)

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
        if self.cur_size+SPACE_LEFT > BUFFER_SIZE:
            return False
        return True

    """
        will detect if the buffer pool is full and write it into the buffer pool
    """
    def update(self, pages_id, pages, table_name):
        while not self.bufferpool_capacity():
            self.flush_page(len(pages.pages), table_name)
        self.cur_size += len(pages.pages)
        self.bufferpool.update({pages_id: pages})
        self.replace.use_append(pages_id)

    def get_pages(self, pages_id, table_name):
        if pages_id in self.bufferpool:
            return self.bufferpool[pages_id]
        else:
            pages = self.disk.readPage(pages_id, table_name)
            self.bufferpool.update({pages_id:pages})
        self.replace.use_append(pages_id)
        if pages is not None:
            return pages

    def size(self):
        return len(self.bufferpool)

class BufferManager:
    def __init__(self, table_name):
        self.buffer = Buffer()
        self.cur_counter = 0
        self.table_name = table_name

    def update(self, pages_id, pages):
        self.buffer.update(pages_id,pages, self.table_name)

    def get_pages(self, pages_id):
        pages = self.buffer.get_pages(pages_id, self.table_name)
        return pages

class LRU:
    def __init__(self):
        self.old = []

    def use_append(self, pages_id):
        if pages_id in self.old:
            self.old.remove(pages_id)
        self.old.append(pages_id)

    def pop_top(self):
        return self.old.pop(0)
