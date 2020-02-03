from template.page import *
from time import time
from template.config import *
from template.index import *


class Record:

    def __init__(self, key, rid, indirect, schema_encode, now, columns, datas):
        self.record = {}
        self.create_record(rid, key, columns, schema_encode, now, indirect, datas)

    def create_record(self, rid, key, columns, schema_encode, now, indirect, datas):
        self.record.update({'key': key})
        self.record.update({'rid': rid})
        self.record.update({'columns': columns})
        self.record.update({'schema': schema_encode})
        self.record.update({'time': now})
        self.record.update({'indirect': indirect})
        self.record.update({'data': datas})



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
        self.base_rid_lookup = {}
        self.base_index_lookup = {}

        self.tail_rid_lookup = {}
        self.tail_index_lookup = {}

        pass

    def __merge(self):
        pass

    def write(self, record, modify_page=TYPE.BASE):
        page = self.load_page(self.num_columns + INTER_DATA_COL, modify_page)

        start_index = page.physical_addr
        page.write(record.record['key'])
        page.write(record.record['rid'])
        page.write(record.record['indirect'])
        page.write(record.record['schema'])
        page.write(record.record['time'])
        page.write(record.record['columns'])
        for i in range(len(record.record['data'])):
            page.write(record.record['data'][i])
        end_index = page.physical_addr

        if TYPE.BASE == modify_page:
            self.base_rid_lookup.update({record.record['key']: record.record['rid']})
            self.base_index_lookup.update({record.record['rid']: Index(self.base_page, start_index, end_index)})
        else:
            self.tail_rid_lookup.update({record.record['key']: record.record['rid']})
            self.tail_index_lookup.update({record.record['rid']: Index(self.tail_page, start_index, end_index)})


    def modify(self, record, index):
        load_base_page = self.page_directory[index.page_number]
        data = translate_data(load_base_page.data[index.start_index:index.end_index+DATA_SIZE])
        old_record = Record(data[0], data[1], data[2], data[3], data[4], data[5], data[6:])
        new_record = self.update_with_schema(record, old_record)

        # write a new tail page
        self.write(new_record, TYPE.TAIL)
        tail_rid = self.tail_rid_lookup[record.record['key']]
        if tail_rid is None :
            print("can't find any tail rid match with key")
            return

        # change indirection for old data
        indirection = tail_rid
        load_base_page.modify(index, indirection)

    def update_with_schema(self, record, old_record):
        ret_record = record
        for key, value in ret_record.record.items():
            if key is 'data':
                ret_data = self.update_data(record, old_record)
                ret_record.record.update({key: ret_data})
            if key is 'key':
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
