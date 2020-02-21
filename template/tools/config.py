# Global Setting for the Database
# PageSize, StartRID, etc..
import time
import enum
# milestone 1
INTER_DATA_COL = 5
DATA_SIZE = 8
NUM_PAGE_RECORDS = 51
DEFAULT_PAGE = 1000
DEFAULT_LOCATION = 1000
BASE_PAGE_NUM = 0
TAIL_PAGE_NUM = 1000
INDIRECTION_INDEX = 16
BUFFERPOOL_SIZE = 100

# milestone 2
BUFFERPOOL_SIZE = 100


TOTAL_RID = 100000000
CURRENT_RID = 1

class TYPE(enum.Enum):
    BASE = 0
    TAIL = 1


"""
    translate_data function used to translate a data from page to a readable data for user
"""


def translate_data(data):
    ret_list = []
    for i in range(10):
        record_str = ''
        for j in range(8):
            temp_str = str(data[i * 8 + j])
            if data[i * 8 + j] < 10:
                record_str += '0' + temp_str
            elif 255 == data[i * 8 + j]:
                break
            else:
                record_str += temp_str
        ret_list.append(int(record_str))
    return ret_list


