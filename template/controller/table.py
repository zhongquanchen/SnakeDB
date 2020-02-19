from template.model.page import *
from time import time
from template.tools.config import *
from template.model.index import *

"""
    :Record is used as a format to write into the page, [this is for admin use]
"""
class Record:
    def __init__(self, key, rid, indirect, schema_encode, now, columns, datas):
        self.record = {}
        self.create_record(rid, key, columns, schema_encode, now, indirect, datas)

    def create_record(self, rid, key, columns, schema_encode, now, indirect, datas):
        self.key = key
        self.rid = rid
        self.columns = columns
        self.schema = schema_encode
        self.time = now
        self.indirect = indirect
        self.datas = datas

"""
    :Record is used as a format to write into the page, [this is for user] *block some data that shouldn't be seem
"""
class Record_For_User:
    def __init__(self, key, rid, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

"""
    :Table will used to store page directories, and all the ids, rid, columns, etc.
"""
class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.base_page = BASE_PAGE_NUM
        self.tail_page = TAIL_PAGE_NUM

        self.name = name
        self.key = key
        self.num_columns = num_columns

        self.page_directory = {}
        self.col_to_key = {}
        self.base_rid_lookup = {}
        self.base_index_lookup = {}

        self.tail_rid_lookup = {}
        self.tail_index_lookup = {}

        # use to match key with rid
        self.key_to_rid = {}
        # use to match the rid and the index, data location
        self.rid_to_index = {}
        # current page index
        self.current_page = 0
        pass

    def __merge(self):
        pass

    """
    :param name: record. contain the data for each rows
    :param name: modify_page. detect the action is to write in base page or in tail page
                              by default the action is writing in base page
    """
    def write(self, record, modify_page=TYPE.BASE):
        record_array = self.record_to_array(record)
        # update key in database & look_for_page will check for the pages directory and update if needed
        pages = self.look_for_pages(modify_page)
        start_index = pages[0].physical_addr
        for i in range(len(pages)):
            pages[i].write(record_array[i])
        end_index = pages[0].physical_addr

        if modify_page == TYPE.BASE:
            self.key_to_rid.update({record.key: record.rid})
            self.rid_to_index.update({record.rid: Index(self.current_page, start_index, end_index)})
        else :
            self.tail_rid_lookup.update({record.key: record.rid})
            self.tail_index_lookup.update({record.rid: Index(self.tail_page, start_index, end_index)})


    """
        format of array 
        # self.key = key
        # self.rid = rid
        # self.columns = columns
        # self.schema = schema_encode
        # self.time = now
        # self.indirect = indirect
        # self.datas = datas
    """
    def record_to_array(self, record):
        record_array = []
        record_array.append(record.key)
        record_array.append(record.rid)
        record_array.append(record.columns)
        record_array.append(record.schema)
        record_array.append(record.time)
        record_array.append(record.indirect)
        for i in range(len(record.datas)):
            record_array.append(record.datas[i])
        return record_array

    def look_for_pages(self, modify_page=TYPE.BASE):
        if TYPE.BASE == modify_page:
            if len(self.page_directory) != 0:
                pages = self.page_directory[self.current_page]
            if len(self.page_directory) == 0 or pages[0].has_capacity() == False:
                pages = []
                for i in range(self.num_columns + INTER_DATA_COL):
                    page = Page()
                    pages.append(page)
                self.current_page += 1
                self.page_directory.update({self.current_page: pages})
            return self.page_directory[self.current_page]
        else:
            # if the dictionary is empty just create one page
            if self.tail_page in self.page_directory:
                pages = self.page_directory[self.tail_page]
            if self.tail_page not in self.page_directory or pages[0].has_capacity() == False:
                pages = []
                for i in range(self.num_columns + INTER_DATA_COL):
                    page = Page()
                    pages.append(page)
                self.tail_page += 1
                self.page_directory.update({self.tail_page: pages})
            return self.page_directory[self.tail_page]

    """
        This function will be called by function[ write() ]
        Is used to load a actual page for write function
    """
    def load_page(self, columns, modify_page=TYPE.BASE):
        if TYPE.BASE == modify_page:
            # if the dictionary is empty just create one page
            if len(self.page_directory) == 0:
                page = Page()
                self.page_directory.update({self.base_page: page})
                return self.page_directory[self.base_page]

            # if dict is not empty then check if the page is full
            page = self.page_directory[self.base_page]
            if not page.has_capacity(columns):
                self.base_page += 1
                page = Page()
                self.page_directory.update({self.base_page: page})
                return self.page_directory[self.base_page]

            # otherwise return this page
            return page
        else:
            # if the dictionary is empty just create one page
            if TAIL_PAGE_NUM not in self.page_directory:
                page = Page()
                self.page_directory.update({self.tail_page: page})
                return self.page_directory[self.tail_page]

            # if dict is not empty then check if the page is full
            page = self.page_directory[self.tail_page]
            if not page.has_capacity(columns):
                self.tail_page += 1
                page = Page()
                self.page_directory.update({self.tail_page: page})
                return self.page_directory[self.tail_page]
            # otherwise return this page
            return page



    """
    :param name: record. contain the data for each rows
    :param name: index. (actually location in the bytearray) used to write into the page
    """
    def modify(self, key, new_record, index):
        rid = self.key_to_rid[key]
        index = self.rid_to_index[rid]
        pages = self.page_directory[index.page_number]
        pages[5].modify(index, new_record.rid)

    """
        This function will check for the row of data to see 
        if the indirection of that data is point to other location
        :param name: page_data. a row of data, [namely: record]
    """
    def check_for_update(self, page_data):
        list_data = translate_data(page_data)
        if list_data[2] != 0:
            index = self.tail_index_lookup[list_data[2]]
            if list_data[2] not in self.tail_index_lookup:
                print("can't find rid in tail page : def check_for_update()")
            tail_page = self.page_directory[index.page_number]
            tail_page_data = translate_data(tail_page.data[index.start_index: index.end_index+DATA_SIZE])
            return tail_page_data
        else:
            return list_data

    """
        This function will be called by function[ modify(self, record, index) ]
        is used to update the schema of a record to check which data needs update in row
        :param name : record, old_record -- namely, new record and old record
    """
    def update_with_schema(self, record, old_record):
        ret_record = record
        for key, value in ret_record.record.items():
            if key == 'data':
                ret_data = self.update_data(record, old_record)
                ret_record.record.update({key: ret_data})
            if key == 'key':
                if ret_record.record[key] is not None:
                    ret_record.record.update({key: ret_record.record[key]})
                else:
                    ret_record.record.update({key: old_record.record[key]})
        return ret_record

    def update_data(self, record, old_record):
        update_data = record.record['data']
        old_data = old_record.record['data']
        ret_data = []
        for i in range(len(update_data)):
            if update_data[i] is None:
                ret_data.append(old_data[i])
            else:
                ret_data.append(update_data[i])
        return ret_data


