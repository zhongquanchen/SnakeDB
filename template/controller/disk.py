from template.controller.table import Table
from template.model.page import *
import os
from os import path


class disk:
    def __init__(self, page_size):
        self.page_size = page_size

    def writePage(self, page):
        filename = str(page.id)
        f = open(filename, 'wb')
        f.write(page.data)
        f.close()
        
    def readPage(self, pageID)
        filename = str(pageID)
        f = open(filename, 'rb')
        p = Page()
        p.physical_addr = 0
        p.num_records = 0
        p.data = f.read()
        p.id = pageID
        p.dirty = False
        p.pin_num = 0
        return page

    def deletePage(self, page):
        filename = str(page.id)
        if path.exists(filename)
            os.remove(filename)
        else:
            pass
        
