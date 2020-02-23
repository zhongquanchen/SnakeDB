from template.controller.table import Table
from template.tools.config import *
from template.controller.buffer import  *
import os
 
class Database():

    def __init__(self):
        self.tables = []
        pass

    def open(self, path):
        try:
            os.mkdir(path)
        except OSError:
            print("Creation of the directory %s failed" % path)
        else:
            print("Successfully created the directory %s " % path)

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