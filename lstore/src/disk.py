# from template.controller.table import Table
from turtledemo.penrose import f


import os
from os import path
import pickle


class disk:
    def __init__(self, page_size):
        self.page_size = page_size
        self.page_record = {}

# pages : class Pages {pages, pages_id}
    def writePage(self, pages, nameTable):
        self.page_record.update({pages.id:pages.pages[0].num_records})
        path = 'ECS165/' + str(nameTable)
        if not os.path.exists(path):
            os.makedirs(path)
        
        cwd = os.getcwd() #get the current working path
        filename = cwd + '/ECS165/' + str(nameTable) + '/' + str(pages.pid)

        f = open(filename, 'wb')
        pickle.dump(pages, f)
        f.close()
        
    def readPage(self, pages_id):
        cwd = os.getcwd() #get the current working path
        filename = cwd + '/ECS165/' + str(nameTable) + '/' + str(pages_id)
        f = open(filename, 'rb')
        
        pages = pickle.load(open(filename, 'rb'))
        return pages

    def deletePage(self, pages):
        cwd = os.getcwd() #get the current working path
        filename = cwd + '/ECS165/' + str(nameTable) + '/' + str(pages.pages_id)
        if path.exists(filename):
            os.remove(filename)
        else:
            pass

    def writeTable(self, table):
        filename = cwd + '/ECS165/' + str(table.name) + '/' + str(table.name)
        with open(filename, 'wb') as f:
            pickle.dump(table, f)
        f.close()

    def readTable(self, tableName):
        filename = cwd + '/ECS165/' + str(table.name) + '/' + str(table.name)
        table = pickle.load(open(filename, 'rb'))
        f.close()
        return table

    def deleteTable(self, table):
        filename = cwd + '/ECS165/' + str(table.name) + '/' + str(table.name)
        if path.exists(filename):
            os.remove(filename)
        else:
            pass
        
