from template.controller.table import Table
from template.model.page import Page
import os
from os import path
import pickle


class disk:
    def __init__(self, page_size):
        self.page_size = page_size

    def writePage(self, page):
        filename = str(page.id)
        f = open(filename, 'wb')
        f.write(page.data)
        f.close()
        
    def readPage(self, pageID):
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

    def writeTable(self, table):
        filename = table.name
        with open(filename, 'wb') as f
            pickle.dump(table, f)
        f.close()

    def readTable(self, tableName):
        filename = tableName
        table = pickle.load(open(filename, 'rb'))
        f.close()
        return table

    def deleteTable(self, table):
        filename = table.name
        if path.exists(filename)
            os.remove(filename)
        else:
            pass
        
