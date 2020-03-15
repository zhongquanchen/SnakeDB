from lstore.src import table
from lstore.src.table import *


# def __init__(self, name, num_columns, key):
class redo:
    def __init__(self):
        self.data = {}
        self.table = None

    def setup_table(self, table):
        self.table = table

    def write_record(self, key, record):
        records = self.data[key]
        records.append(record)
        self.data.update({record.key:records})

    def insert_base(self, record):
        self.data.update({record.key : [record]})

    def roll_back_action(self, keys):
        records = []
        for key in keys:
            items = self.data[key]
            item = items[0]
            self.table.modify(key, item)
            self.table.write(item, TYPE.TAIL)

    def commit(self, keys):
        for key in keys:
            items = self.data[key]
            item = items[len(items)-1]
            self.data.update({item.key:[item]})



REDOLOG = redo()


