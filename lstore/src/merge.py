from lstore.src.config import *
from lstore.src.query import *
from lstore.src.table import *
from lstore.src.page import *
from time import *
from lstore.src.table import *
import threading

# 1. have the base page and tail merge together
     #. need to run in background
     #. all the base page will copy to a new buffer pool

# 2. have new update tail record point to baseRID
     #. modify the base-merged record indirection to the new update tail record during merge

class merge :

    def __init__(self, base_page):
        self.locking = False
        self.thread = threading.Thread(target=self.merged())
        self.old_base = base_page
        self.new_base = base_page
        self.locking = False

    # start a merge process
    def merge_process(self):
        self.locking = True
        self.thread.start()

    # end will join the process
    def merged(self):
        self.thread.join()
        self.locking = False

    def tail_record_points_back(self):
        pass

    def run_all_base_page(self):
        for key in self.table.key_to_rid:
            print("all the keys ", key, " rids: ", self.table.key_to_rid[key])

    # copy the base page
    def copy_base_page(self, old_page, new_page):
        merge.run_all_base_page(old_page);
        new_page = old_page
        return new_page
