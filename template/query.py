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

    def insert(self, *columns):
        key = columns[0]  # the first of the column is key from user input
        rid = key % 906659671
        schema_encoding = '0' * (self.table.num_columns+5)
        num_columns = self.table.num_columns+4
        cur_time = int(time.time()) # unable to store float, so convert to int type
        indirect = 0

        # (rid, key, columns, schema_encode, now, indirect, *datas)
        record = Record(rid, key, num_columns, schema_encoding, cur_time, indirect, list(columns[1:]))

        # print(record.key)
        # print(record.columns)

        self.table.write(record)

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        rid = self.table.rid_lookup[key]
        page_num = int(rid / NUM_PAGE_RECORDS)
        # print(key, end=' ')
        # print(rid, end=' ')
        # print(page_num)
        page = self.table.page_directory[page_num]

        record_index = self.table.index_lookup[rid]
        page_data = page.data[record_index*9:record_index*9+72]
        return page_data


    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        pass


    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
