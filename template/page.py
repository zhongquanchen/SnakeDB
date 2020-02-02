from template.config import *


class Page:

    def __init__(self):
        self.location = 0
        self.num_records = 0
        self.data = bytearray(4096)
        self.index_lookup = {}

    def has_capacity(self, columns):
        if self.num_records + columns >= 512:
            return False
        return True

    def write(self, value, location):
        str_val = str(value)
        if location >= DEFAULT_LOCATION:
            value_list = self.convert_8byte(str_val)
            for i in range(len(value_list)):
                self.data[self.num_records * 8 + i] = value_list[i]
            self.num_records += 1
        else : # when it needs to modify the old data
            value_list = self.convert_8byte(str_val)
            for i in range(len(value_list)):
                self.data[location * 8 + i] = value_list[i]

    def convert_8byte(input):
        hex_list = []
        hex_input = list(input)
        if len(hex_input) % 2 != 0:
            hex_input.insert(0, '0')

        if len(hex_input) / 2 > 8:
            print("value exceed 8bytes")
            return

        for i in range(int(len(hex_input) / 2)):
            temp_str = hex_input[i * 2] + hex_input[i * 2 + 1]
            hex_list.append(int(temp_str))
        hex_list.append(255)
        return hex_list
