from template.page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns, schema_encode, now, indirect, datas):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.schema = schema_encode
        self.cur_time = now
        self.indir_col = indirect
        self.data = datas


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        self.cur_page = 0
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        pass

    def __merge(self):
        pass

    def write(self, record):
        page = self.load_page(record.columns)
        #  rid, key, columns, schema_encode, now, indirect, *datas):
        page.write(record.rid)
        page.write(record.key)
        page.write(record.columns)
        page.write(record.schema)
        page.write(record.cur_time)
        page.write(record.indir_col)

        for i in range(len(record.data)):
            page.write(record.data[i])


    def load_page(self, columns):
        # if the dictionary is empty just create one page
        if len(self.page_directory) == 0:
            page = Page()
            self.page_directory.update({self.cur_page : page})
            return self.page_directory[self.cur_page]

        # if dict is not empty then check if the page is full
        page = self.page_directory[self.cur_page]
        if page.has_capacity(columns) == False:
            self.cur_page += 1
            page = Page()
            self.page_directory.update({self.cur_page : page})
            return self.page_directory[self.cur_page]

        # otherwise return this page
        return page
