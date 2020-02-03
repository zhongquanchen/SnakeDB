from template import table
from template.table import Table, Record
from template.index import Index
from template.page import *
from template.config import *


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        self.num_col = 0
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        # delete data with key
        if key in self.table.base_rid_lookup:
            try:
                del self.table.base_rid_lookup[key]
            except KeyError:
                print("Key is not Found")
        if key in self.table.tail_rid_lookup:
            try:
                del self.table.tail_rid_lookup[key]
            except KeyError:
                print("Key is not Found")


    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        key = columns[0]  # the first of the column is key from user input
        if key in self.table.base_rid_lookup:
            print("key existed in db")
            return

        rid = key % 906659671
        schema_encoding = '0' * self.table.num_columns
        cur_time = int(time.time())  # unable to store float, so convert to int type
        indirect = 0
        record = Record(key, rid, indirect, schema_encoding, cur_time, self.num_col, list(columns[1:]))
        self.table.write(record)
        self.num_col += 1

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        if key not in self.table.base_rid_lookup:
            print("can't find key")
            exit()
        page_data = self.select_bytearray(key)
        newest_data = self.check_for_update(page_data)
        temp_list = translate_data(newest_data)


        # list_for_user = []
        # list_for_user.append(temp_list[0])
        # for i in range(self.table.num_columns-1):
        #     list_for_user.append(temp_list[i+INTER_DATA_COL+1])
        return temp_list

    # FIXME: NEED TO FILTER OUT THE QUERY_COL

    def select_bytearray(self, key):
        rid = self.table.base_rid_lookup[key]
        index = self.table.base_index_lookup[rid]
        page = self.table.page_directory[index.page_number]
        page_data = page.data[index.start_index: index.end_index]
        return page_data

    def check_for_update(self, page_data):
        list_data = translate_data(page_data)
        if list_data[2] != 0:
            index = self.table.tail_index_lookup[list_data[2]]
            if list_data[2] not in self.table.tail_index_lookup:
                print("can't find rid in tail page : def check_for_update()")
            tail_page = self.table.page_directory[index.page_number]
            tail_page_data = tail_page.data[index.start_index: index.end_index+DATA_SIZE]
            return tail_page_data
        else:
            return page_data



    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        # look up the data location
        rid = self.table.base_rid_lookup[key]
        index = self.table.base_index_lookup[rid]
        page = self.table.page_directory[index.page_number]
        page_data = page.data[index.start_index: index.end_index]

        # translate data from bytearray
        data = translate_data(page_data)
        new_rid = data[1] * 10 + 1
        new_scheme = self.modify_schema(list(columns))
        modify_record = Record(columns[0], new_rid, data[2], new_scheme, data[4], data[5], list(columns[1:]))
        # modify data with key
        self.table.modify(modify_record, index)


    def modify_schema(self, columns):
        schema = ''
        for i in range(len(columns)):
            if columns[i] is None:
                schema += '0'
            else:
                schema += '1'
        return schema



    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        from_num = 0
        to_num = 0
        if start_range > end_range:
            from_num = end_range
            to_num = start_range
        else :
            from_num = start_range
            to_num = end_range

        pages = self.find_page_dir(from_num, to_num)



    def find_page_dir(self, from_num, to_num):
        pages = []
        num_record_in_page = self.table.page_directory[0].page_num / (self.table.num_columns + INTER_DATA_COL)
        print("number of record in a page ", num_record_in_page)

        contain_page_num = 0
        # search for the base page range
        for i in range(TAIL_PAGE_NUM):
            if i in self.table.page_directory:
                if from_num < i * 51 < to_num:
                    pages.append(self.table.page_directory[i])
                elif i * 51 > to_num:
                    break
        # return pages within range of index
        return pages

