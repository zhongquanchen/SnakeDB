import operator
from lstore.src.table import *
from lstore.src.page import *
from lstore.src.config import *
from lstore.src.buffer import *


class Query:
    """ Creates a Query object that can perform different queries on the specified table """

    def __init__(self, table):
        self.table = table
        self.num_col = 0
        self.update_counter = 0
        pass

    """ Delete the key in the dictionary, throw an exception when user want to update the deleted record """

    def delete(self, key):
        # delete data with key in base page
        if key in self.table.key_to_rid:
            try:
                del self.table.key_to_rid[key]
            except KeyError:
                print("Key is not Found")

        # delete data with key in tail page
        if key in self.table.tail_rid_lookup:
            try:
                del self.table.tail_rid_lookup[key]
            except KeyError:
                print("Key is not Found")

    """ Insert a record with specified columns """
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

    def insert(self, *columns):
        key = columns[0]
        cur_time = int(time.time())
        rid = key % 100000000 # we use rid as the current, so when ever it create, it is unique
        indirect = 0
        schema_encoding = '0' * self.table.num_columns
        record = Record(key, rid, indirect, schema_encoding,
                        cur_time, self.num_col, list(columns[1:]))
        self.num_col += 1
        schema_encoding = '0' * self.table.num_columns
        self.table.write(record)

    """ Select a record with specified columns"""

    def select(self, key, column, query_columns):
        data = self.find_data_by_key(key)
        if 0 != data[5]:
            data = self.check_for_update(data[5])
        ret_data = [data[0]]
        ret_data = ret_data + data[6:]
        record = Record_For_User(ret_data[0], ret_data[1], ret_data)
        list_data =[record]
        return list_data

    """ Update a record with specified key and columns """

    def update(self, key, *columns):
        rid = self.table.key_to_rid[key]  # look up the data location
        index = self.table.rid_to_index[rid]
        old_data = self.find_data_by_key(key)
        new_data = self.combine_old_data(old_data, *columns)
        new_record = Record(new_data[0], new_data[1], new_data[2], new_data[3], new_data[4], new_data[5], new_data[6:])
        # print("list is ", new_data)
        self.table.modify(key, new_record, index)
        self.table.write(new_record, TYPE.TAIL)

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        # sort the key in dictionary
        # sum them up by two range
        sum = 0
        sorted_keys = sorted(
            self.table.key_to_rid.items(), key=operator.itemgetter(0))
        on_add = False
        for i in range(end_range - start_range+1):
            if start_range + i in self.table.key_to_rid:
                data = self.select(start_range + i, 0, [1, 1, 1, 1, 1])[0]
                sum += data.columns[aggregate_column_index]
        return sum


    """
    an extension to help implement above's functions
    """
    def find_keys(self, start_range, end_range):
        a = 0
        b = 0
        for key, value in self.table.col_to_key.items():
            if value == start_range:
                a = key
            if value == end_range:
                b = key
        return [a, b]

    def check_for_update(self, rid):
        if rid not in self.table.tail_index_lookup:
            print("can't find key in tail")
            exit()
        page_dir = self.table.tail_index_lookup[rid]
        pages_id = self.table.page_directory[page_dir.page_number]
        pages = self.table.buffer_manager.get_pages(pages_id)
        data = []
        for i in range(len(pages.pages)):
            data.append(self.read_data(pages.pages[i], page_dir.start_index, page_dir.end_index))
        return data

    def find_data_by_key(self, key):
        if key not in self.table.key_to_rid:
            print("can't find key")
            exit()
        rid = self.table.key_to_rid[key]
        page_dir = self.table.rid_to_index[rid]
        pages_id = self.table.page_directory[page_dir.page_number]
        pages = self.table.buffer_manager.get_pages(pages_id)
        data = []
        for i in range(len(pages.pages)):
            data.append(self.read_data(pages.pages[i], page_dir.start_index, page_dir.end_index))
        return data

    def read_data(self, page, start_index, end_index):
        # this is raw data, not handled
        data = page.read_data(start_index, end_index)
        ret_data = ''
        for i in range(len(data)):
            if data[i] < 10:
                ret_data += '0' + str(data[i])
            elif 255 == data[i]:
                break
            else:
                ret_data += str(data[i])
        return int(ret_data)

    def combine_old_data(self, old_data, *columns):
        self.update_counter += 1
        if old_data[5] != 0:
            old_data = self.check_for_update(old_data[5])
        filtered_data = old_data
        if columns[0] is not None:
            filtered_data[0] = columns[0]

        for i in range(len(columns)-1):
            if columns[i+1] is not None:
                filtered_data[6+i] = columns[i+1]
        filtered_data[1] = int(old_data[1]) + self.update_counter
        return filtered_data