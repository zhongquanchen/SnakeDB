from template.tools.config import *
from template.model.page import *
from template.controller.disk import *
from template.controller.replace import *

class buffer:
    def __init__(self):
<<<<<<< Updated upstream
        self.buffer = []
        self.disk = disk()
=======
        # buffer pages
        self.bufferpool = {}
        self.buffersize = BUFFER_SIZE
        self.cur_size = 0
        self.disk = disk(100)
        self.replace = LRU()
>>>>>>> Stashed changes

    def fetch_page(self, page_id):
        for pages in self.buffer:
            if pages.id == page_id:
                return pages
        page = self.replace.evict()
        if page.is_dirty():
            self.disk.writePage(page)

    # Close function
<<<<<<< Updated upstream
    def flush_page(self, page_id):
        for pages in self.buffer:
            if pages.id == page_id:
                self.disk.writePage(pages)
                return True
        return False
=======
    def flush_page(self, pages_len):
        old_data = self.replace.pop_top()
        if old_data in self.bufferpool:
            self.disk.writePage(self.bufferpool[old_data])
            del self.bufferpool[old_data]
            self.cur_size -= pages_len
>>>>>>> Stashed changes

    def delete_page(self, page_id):
        for pages in self.buffer:
            if pages.id == page_id:
<<<<<<< Updated upstream
                self.disk.deletePage(pages)
=======
                self.disk.delete_page(pages)

    def unpinning_page(self, page_id, is_dirty):
        for Pages in self.bufferpool:
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
    def update(self, pages_id, pages):
        while not self.bufferpool_capacity():
            print("size not enough")
            self.flush_page(len(pages.pages))

        self.cur_size += len(pages.pages)
        self.bufferpool.update({pages_id: pages})
        self.replace.use_append(pages_id)

    def get_pages(self, pages_id):
        if pages_id in self.bufferpool:
            return self.bufferpool[pages_id]
        else:
            pages = self.disk.readPage(pages_id)
            self.bufferpool.update({pages_id:pages})

        self.replace.use_append(pages_id)
        if pages is not None:
            return pages

class BufferManager:
    def __init__(self):
        self.buffer = Buffer()
        self.cur_counter = 0

    def update(self, pages_id, pages):
        self.buffer.update(pages_id,pages)

    def get_pages(self, pages_id):
        pages = self.buffer.get_pages(pages_id)
        return pages

>>>>>>> Stashed changes

class LRU:
    def __init__(self):
        self.old = []

    def use_append(self, pages_id):
        if page in self.old:
            self.old.remove(pages_id)
        self.old.append(pages_id)

    def pop_top(self):
        return self.old.pop(0)

    # def access(self, page):
    #     if page in self.buffer:
    #         self.use_append(page)
    #         return
    #     if None in self.buffer:
    #         self.buffer[self.buffer.index(None)] = page
    #         self.use_append(page)
    #         return
    #     assert (len(self.old) > 0)
    #     is_replace = self.old.pop(0)
    #     self.buffer[self.buffer.index(is_replace)] = page
    #     self.use_append(page)
