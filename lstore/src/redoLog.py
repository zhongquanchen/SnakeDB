from lstore.src import table
from lstore.src.table import *


# def __init__(self, name, num_columns, key):
class redo:
    def __init__(self):
        self.data = {}
        self.table = Table("empty", 5, 0)

    def write_record(self, record):
        self.data.update({record.key : record})

    def roll_back_action(self, keys):
        records = []
        for key in keys:
            record = self.data[key]
            records.append(record)

        if self.table.name != "empty":
            for record in records:
                self.table.modify(key, record)
                self.table.write(record, TYPE.TAIL)
        else:
            print("fail to roll back, the table is empty")
            return False
        return True


REDOLOG = redo()
# records
# class query->update => REDOLOG.write_record
#
# class transaction->abort => roll_back()

