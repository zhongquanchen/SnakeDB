import operator
from template import table
from template.table import Table, Record, Record_For_User
from template.index import Index
from template.page import *
from template.config import *


class Query:
    """ Creates a Query object that can perform different queries on the specified table """
    def __init__(self, table):
        self.table = table
        self.num_col = 0
        pass

    """ Delete the key in the dictionary, throw an exception when user want to update the deleted record """
    def delete(self, key):
        # delete data with key in base page
        if key in self.table.base_rid_lookup:
            try:
                del self.table.base_rid_lookup[key]
            except KeyError:
                print("Key is not Found")
        # delete data with key in tail page
        if key in self.table.tail_rid_lookup:
            try:
                del self.table.tail_rid_lookup[key]
            except KeyError:
                print("Key is not Found")

    """ Insert a record with specified columns """
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

    """ Select a record with specified columns"""
    def select(self, key, query_columns):
        if key not in self.table.base_rid_lookup:
            print("can't find key")
            exit()
        page_data = self.select_bytearray(key)
        newest_data = self.check_for_update(page_data)
        temp_list = translate_data(newest_data)
        list_for_user = []
        list_for_user.append(temp_list[0])
        for i in range(self.table.num_columns-1):
            list_for_user.append(temp_list[6+i])
        record = Record_For_User(temp_list[0], temp_list[1], list_for_user)
        other_list = []
        other_list.append(record)
        return other_list

    def select_bytearray(self, key):  # Select the page data
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

    """ Update a record with specified key and columns """
    def update(self, key, *columns):
        rid = self.table.base_rid_lookup[key]  # look up the data location
        index = self.table.base_index_lookup[rid]
        page = self.table.page_directory[index.page_number]
        page_data = page.data[index.start_index: index.end_index]
        data = translate_data(page_data)  # translate data from bytearray
        new_rid = data[1] * 10 + 1
        new_scheme = self.modify_schema(list(columns))
        modify_record = Record(columns[0], new_rid, data[2], new_scheme, data[4], data[5], list(columns[1:]))
        self.table.modify(modify_record, index)  # modify data with key

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
        num_record_in_page = self.table.page_directory[0].num_records / (self.table.num_columns + INTER_DATA_COL)
        keys_col = self.find_keys(start_range, end_range)  # find two key
        sum = 0
        sorted_keys = sorted(self.table.base_rid_lookup.items(), key=operator.itemgetter(0))
        on_add = False
        for i in range(end_range - start_range+1):
            if start_range + i in self.table.base_rid_lookup:
                data = self.select(start_range + i, [1,1,1,1,1])[0]
                sum += data.columns[aggregate_column_index]
        return sum

    def find_keys(self, start_range, end_range):
        a = 0
        b = 0
        for key, value in self.table.col_to_key.items():
            if value == start_range:
                a = key
            if value == end_range:
                b = key
        return [a, b]




