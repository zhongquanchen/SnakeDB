from template.table import Table, Record
from template.index import Index
from template.page import *

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
        self.key = key
        record = Record(key)
        self.table.remove(key)

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        rid = 0
        keys = columns[0]
        schema_encoding = '0' * self.table.num_columns

        record = Record(rid, columns[0], self.table.num_columns + 4 )

        pass

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        pass

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
