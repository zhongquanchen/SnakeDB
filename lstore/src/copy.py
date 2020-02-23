from lstore.src.merge import *


class copy:

    def __init__(self):
        self.copy = False;

    # copy the base page
    def copy_base_page(self, old_page, new_page):
        merge.run_all_base_page(old_page);
        new_page = old_page
        return new_page
