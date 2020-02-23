from lstore.src.config import *
from lstore.src.table import *
from lstore.src.page import *


import threading

# 1. have the base page and tail merge together
     #. need to run in background
     #. all the base page will copy to a new buffer pool

# 2. have new update tail record point to baseRID
     #. modify the base-merged record indirection to the new update tail record during merge

class MergeManager :

    def __init__(self):
        self.locking = False
        self.copy_pages = []

    # start a merge process
    def merge_process(self):
        self.thread = threading.Thread(target=self.merge)
        self.locking = True
        if not self.thread.is_alive():
            self.thread.start()
        return True

    def join_self(self):
        self.thread.join()

    # end will join the process
    def merge(self):
        print("start merging process")
        self.locking = False


    # copy the base page
    def copy_base_page(self, table):
        # loop through page directory, find all base page
        for key in table.key_to_rid:
            index = table.rid_to_index[key]
            pid = table.page_directory[index.page_number]
            pages = table.buffer_manager.get_pages(pid)
            if pages in self.copy_pages:
                print("pages already exist in copylist")
            else:
                self.copy_pages.append(pages.copy())
        return
