from template.config import *


class Page:

    def __init__(self):
        self.physical_addr = 0
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self, columns):
        if self.num_records + columns >= 512:
            return False
        return True

    def write(self, value):
        num_addr = self.num_records * 8
        str_val = str(value)
        value_list = self.convert_8byte(str_val)
        for i in range(len(value_list)):
            self.data[num_addr + i] = value_list[i]
        self.num_records += 1
        self.physical_addr = self.num_records * 8

    def modify(self, index, indir):
        str_val = str(indir)
        value_list = self.convert_8byte(str_val)
        for i in range(len(value_list)):
            self.data[index.start_index+INDIRECTION_INDEX+i] = value_list[i]

    def convert_8byte(self, input):
        hex_list = []
        hex_input = list(input)
        if len(hex_input) % 2 != 0:
            hex_input.insert(0, '0')

        if len(hex_input) / 2 > 7:
            print("value exceed 8bytes")
            return

        for i in range(int(len(hex_input) / 2)):
            temp_str = hex_input[i * 2] + hex_input[i * 2 + 1]
            hex_list.append(int(temp_str))
        hex_list.append(255)
        return hex_list
