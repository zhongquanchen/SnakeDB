from template.page import *
from time import time
from template.config import *

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
        self.index = 0

        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.rid_lookup = {}
        self.index_lookup = {}
        pass

    def __merge(self):
        pass

    def write(self, record, location=DEFAULT_LOCATION, type=TYPE.BASE, modify_page=DEFAULT_PAGE):

        page = self.load_page(record.columns, modify_page)
        # updating dictionary on conditions
        if TYPE.BASE == type:
            self.rid_lookup.update({record.key: record.rid})
            self.index_lookup.update({record.rid: page.num_records} )

        # update dict for matching key & rid
        page.write(record.key, location)
        page.write(record.rid, location+1)
        page.write(record.columns, location+2)
        page.write(record.schema, location+3)
        page.write(record.cur_time, location+4)
        page.write(record.indir_col, location+5)
        for i in range(len(record.data)):
            page.write(record.data[i], location+6+i)

    def load_page(self, columns, modify_page=DEFAULT_PAGE):
        if DEFAULT_PAGE == modify_page:
            # if the dictionary is empty just create one page
            if len(self.page_directory) == 0:
                page = Page()
                self.page_directory.update({self.cur_page: page})
                return self.page_directory[self.cur_page]

            # if dict is not empty then check if the page is full
            page = self.page_directory[self.cur_page]
            if not page.has_capacity(columns):
                self.cur_page += 1
                page = Page()
                self.page_directory.update({self.cur_page: page})
                return self.page_directory[self.cur_page]

            # otherwise return this page
            return page
        else :
            return self.page_directory[modify_page]
