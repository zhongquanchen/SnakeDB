import static as static

from lstore.src.table import Table
from lstore.src.disk import *
from lstore.src.config import *
from lstore.src.buffer import *

import os
from os import path
import pickle


class Database:

    def __init__(self):
        self.tables = []
        self.num_columns = 0
        self.disk = disk()
        pass

    def open(self, path):
        if not os.path.exists('ECS165/'):
            os.makedirs('ECS165/')
        self.maxBufferSize = BUFFER_SIZE

    def close(self):
        for table in self.tables:
            self.disk.writeTable(table)


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        cwd = os.getcwd()
        path = 'ECS165/' + str(name)
        if not os.path.exists(path):
            os.makedirs(path)
        filename = cwd + '/ECS165/' + str(table.name) + '/' + str(table.name)
        with open(filename, 'wb') as f:
            pickle.dump(table, f)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        self.disk.deleteTable(name)
        pass

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        table = self.disk.readTable(name)
        return table
