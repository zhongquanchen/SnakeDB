from lstore.src.query import *

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Indexing:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.query = Query(table)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        ret_list = []
        for key in self.query.table.key_to_rid:
            data = self.query.find_data_by_key(key)
            user_data = [data[0]]
            user_data = user_data + data[6:]
            if data[column] == value:
                ret_list.append(user_data)

        temp_list = []
        ret_indice = []
        for i in range(len(self.indices)):
            for j in range(len(ret_list)):
                temp_list.append(ret_list[j][i])
            ret_indice.append(temp_list)
            temp_list = []

        return ret_indice

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        ret_list = []
        for key in self.query.table.key_to_rid:
            data = self.query.find_data_by_key(key)
            user_data = [data[0]]
            user_data = user_data + data[6:]
            if end >= data[column] >= begin:
                for i in range(len(self.indices)):
                    ret_list.append(user_data)

        temp_list = []
        ret_indice = []
        for i in range(len(self.indices)):
            for j in range(len(ret_list)):
                temp_list.append(ret_list[j][i])
            ret_indice.append(temp_list)
            temp_list = []

        return ret_indice

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        ret_list = []
        for key in self.query.table.key_to_rid:
            data = self.query.find_data_by_key(key)
            ret_list.append(data)

        temp_list = []
        ret_indice = []
        for i in range(len(self.indices)):
            for j in range(len(ret_list)):
                temp_list.append(ret_list[j][i])
            self.indices[i](temp_list)
            temp_list = []

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices = [None] * column_number

