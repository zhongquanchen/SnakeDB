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
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        pass

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns, type=TYPE.BASE):
        if type == TYPE.BASE:
            key = columns[0]  # the first of the column is key from user input
            rid = key % 906659671
            schema_encoding = '0' * (self.table.num_columns+5)
            num_columns = self.table.num_columns+4
            cur_time = int(time.time()) # unable to store float, so convert to int type
            indirect = 0
            record = Record(rid, key, num_columns, schema_encoding, cur_time, indirect, list(columns[1:]))
            self.table.write(record)



    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        page_data = self.select_bytearray(key)
        return page_data
    # FIXME: NEED TO FILTER OUT THE QUERY_COL

    def select_bytearray(self, key):
        rid = self.table.rid_lookup[key]
        page_num = int(rid / NUM_PAGE_RECORDS)
        page = self.table.page_directory[page_num]
        record_index = self.table.index_lookup[rid]
        page_data = page.data[record_index*9:record_index*9+72]
        return page_data


    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        # look up the data location
        rid = self.table.rid_lookup[key]
        page_num = int(rid / NUM_PAGE_RECORDS)
        page = self.table.page_directory[page_num]
        record_index = self.table.index_lookup[rid]
        record = page.data[record_index*9:record_index*9+72]

        # translate data from bytearray
        data = self.translate_data(record)

        modify_record = Record(data[1], data[0], data[2], data[3], data[4], data[5], list(columns[1:]))
        # modify data with key
        # write(self, record, location=0, type=TYPE.BASE, modify_page=DEFAULT_PAGE):
        self.table.write(modify_record,record_index, TYPE.TAIL, page_num)
        # print(record_index)



    def translate_data(self, data):
        ret_list = []
        for i in range(9):
            record_str = ''
            for j in range(8):
                temp_str = str(data[i*8+j])
                if data[i*8+j] < 10:
                    record_str += '0' + temp_str
                elif 255 == data[i*8+j]:
                    break
                else:
                    record_str += temp_str
            ret_list.append(int(record_str))
        return ret_list


        # ret_list = []
        # ret_str = ''
        # for i in range(NUM_PAGE_RECORDS):
        #     temp_str = ''
        #     for j in range(8):
        #         temp_str = str(data[i*9+j])
        #         if data[i] < 10:
        #             ret_str += '0' + temp_str
        #         elif data[i] == 255:
        #             ret_list.append(temp_str)
        #             break
        #         else:
        #             ret_str += temp_str
        #     ret_list.append(ret_str)
        # return ret_list

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
