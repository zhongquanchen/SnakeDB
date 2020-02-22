from template.controller.table import Table
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
        page = Page(0, 0, f.read(), pageID, false)
        return page

    def deletePage(self, page):
        filename = str(page.id)
        if path.exists(filename)
            os.remove(filename)
        else:
            pass
        
