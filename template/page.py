from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self, columns):
        if self.num_records+columns >= 512:
            return False
        return True


    def write(self, value):
        value_list = self.convert_8byte(value)
        for i in range(len(value_list)):
            self.data[self.num_records * 8 + i] = value_list[i]
            self.num_records += 1

    def convert_8byte(self, input):
        hex_list = []
        hex_input = list(str(input))
        if len(hex_input) % 2 != 0:
            hex_input.insert(0, '0')

        if len(hex_input) / 2 > 8:
            print("value exceed 8bytes")
            return

        for i in range(int(len(hex_input) / 2)):
            temp_str = hex_input[i * 2] + hex_input[i * 2 + 1]
            hex_list.append(int(temp_str))
        return hex_list
