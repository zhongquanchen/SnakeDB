from lstore.src.table import Table
from lstore.src.config import *
from lstore.src.buffer import  *

class Database():

    def __init__(self):
        self.tables = []
        pass

    def open(self, path):
        #path = table file location

        self.maxBufferSize = BUFFER_SIZE
        self.currentBufferSize = 0
        #NEED TO inlcude Buffersize in Buffer initialization
        self.buffer = Buffer()


    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables
        pass