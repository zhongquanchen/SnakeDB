# Global Setting for the Database
# PageSize, StartRID, etc..
import time
import enum

NUM_PAGE_RECORDS = 51
DEFAULT_PAGE = 1000
DEFAULT_LOCATION = 1000

class TYPE(enum.Enum):
    BASE = 0
    TAIL = 1
