from lstore.src.config import *
from lstore.src.query import *
from lstore.src.table import *
from lstore.src.page import *
# 1. have the base page and tail merge together
     #. need to run in background
     #. all the base page will copy to a new buffer pool

# 2. have new update tail record point to baseRID
     #. modify the base-merged record indirection to the new update tail record during merge

class merge :

    def __init__(self, base_page):
        self.new_base = base_page
        self.old_base = base_page
        pass

    def update_tail_to_new(self):
        for i in range(self.old_base)
            ###todo

    def merged(self):
        pass

    def tail_record_points_back(self):
        pass



