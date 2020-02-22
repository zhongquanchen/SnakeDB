from template.tools.config import *

"""
Implementation of Page for our SnakeSQL
"""

class Page:
    """define page itself with a physical address, numbers of records"""

    def __init__(self):
        self.physical_addr = 0
        self.num_records = 0
        self.data = bytearray(4096)
        self.id = id
        self.dirty = False
        self.pin_num = 0

    def read_data(self, start_index, end_index):
        ret_data = []
        data = self.data[start_index:end_index]
        for i in range(end_index - start_index):
            ret_data.append(data[i])
        return ret_data

    """define the capacity of the page"""

    def has_capacity(self):
        if self.num_records + 1 >= 512:
            return False
        return True

    """define write function to write value into the page"""

    def write(self, value):
        num_addr = self.num_records * 8
        str_val = str(value)
        value_list = self.convert_8byte(str_val)
        for i in range(len(value_list)):
            self.data[num_addr + i] = value_list[i]
        self.num_records += 1
        self.physical_addr = self.num_records * 8

    """define modify function in order to implement update query later"""

    def modify(self, index, indir):
        str_val = str(indir)
        value_list = self.convert_8byte(str_val)
        for i in range(len(value_list)):
            self.data[index.start_index + i] = value_list[i]

    """Write 8 bytes into the page at a time"""

    def convert_8byte(self, input):
        hex_list = []
        hex_input = list(input)
        if len(hex_input) % 2 != 0:
            hex_input.insert(0, '0')

        if len(hex_input) / 2 > 7:
            print("value exceed 8bytes")
            return
        for i in range(int(len(hex_input) / 2)):  # make sure the length of list
            temp_str = hex_input[i * 2] + hex_input[i * 2 + 1]
            hex_list.append(int(temp_str))
        hex_list.append(255)
        return hex_list

    def is_dirty(self):
        return self.dirty

    def increase_pin(self):
        self.pin_num += 1

    def decrease_pin(self):
        self.pin_num -= 1

    def get_pin(self):
        return self.pin_num
