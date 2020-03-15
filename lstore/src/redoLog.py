from lstore.src import table
from lstore.src.table import *


# def __init__(self, name, num_columns, key):
class redo:
    def __init__(self):
        self.data = {}
        self.table = None

    def setup_table(self, table):
        self.table = table

    def write_record(self, record):
        new_record = Record(record[0], record[1], record[2],
               record[3], record[4], record[5], record[6:])

        if new_record.key not in self.data:
            self.data.update({new_record.key : new_record})

    def roll_back_action(self, keys):
        records = []
        for key in keys:
            if key in self.data:
                record = self.data[key]
                records.append(record)

        if self.table is not None:
            for record in records:
                self.table.modify(key, record)
                self.table.write(record, TYPE.TAIL)
                print("roll back action ending")
        else:
            print("fail to roll back, the table is empty")

    def clearList(self):
        self.data.clear()


REDOLOG = redo()


