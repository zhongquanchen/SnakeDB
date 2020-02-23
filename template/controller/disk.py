# from template.controller.table import Table
from turtledemo.penrose import f

from template.model import page
from template.model.page import Page
import os
from os import path
import pickle


class disk:
    def __init__(self, page_size):
        self.page_size = page_size
        self.page_record = {}

# pages : class Pages {pages, pages_id}
    def writePage(self, pages):
        self.page_record.update({pages.id:pages.pages[0].num_records})
        if not os.path.exists('dbFile'):
            os.makedirs('dbFile')
        
        cwd = os.getcwd() #get the current working path
        filename = cwd + '/dbfile/' + str(pages.pid)
        f = open(filename, 'wb')

        for page in pages.pages:   
            #f.write(page.data)
            f.writelines(page.data)
            #f.write(os.linesep) #write a new line sign in the current working OS
        f.close()
        
    def readPage(self, pages_id, num_pages):
        cwd = os.getcwd() #get the current working path
        filename = cwd + '/dbfile/' + str(pages_id)
        f = open(filename, 'rb')
        
        pages = {pages, pages_id}
        pages.pages = []
        
        for i in num_pages:
            p = Page()
            pageData = f.readline()
            p.physical_addr = 0
            p.num_records = 0
            pages.pages.append(page)
        
        return pages

    def deletePage(self, pages):
        cwd = os.getcwd() #get the current working path
        filename = cwd + '/dbfile/' + str(pages.pages_id)
        if path.exists(filename):
            os.remove(filename)
        else:
            pass

    def writeTable(self, table):
        filename = table.name
        with open(filename, 'wb') as f:
            pickle.dump(table, f)
        f.close()

    def readTable(self, tableName):
        filename = tableName
        table = pickle.load(open(filename, 'rb'))
        f.close()
        return table

    def deleteTable(self, table):
        filename = table.name
        if path.exists(filename):
            os.remove(filename)
        else:
            pass
        
