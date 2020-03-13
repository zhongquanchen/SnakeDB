import operator
import concurrent.futures

from lstore.src.redoLog import REDOLOG
from lstore.src.table import *
from lstore.src.buffer import *
from lstore.src.lock import *
from lstore.src.config import *

from random import choice, randint, sample, seed


class Query:
    """ Creates a Query object that can perform different queries on the specified table """

    def __init__(self, table):
        REDOLOG.table = table
        self.MERGE_COUNTER = MERGE_COUNTER
        self.table = table
        self.num_col = 0
        self.update_counter = 0

        self.merge_lock = False
        self.locked = False
        self.update_list = []

        self.merged_page_num = []
        self.merged_pages = []

    """ Delete the key in the dictionary, throw an exception when user want to update the deleted record """

    def delete(self, key):
        # delete data with key in base page
        if key in self.table.key_to_rid:
            try:
                del self.table.key_to_rid[key]
            except KeyError:
                print("Key is not Found")

        # delete data with key in tail page
        if key in self.table.tail_rid_lookup:
            try:
                del self.table.tail_rid_lookup[key]
            except KeyError:
                print("Key is not Found")

    """ Insert a record with specified columns """
    """
        format of array 
        # self.key = key
        # self.rid = rid
        # self.columns = columns
        # self.schema = schema_encode
        # self.time = now
        # self.indirect = indirect
        # self.datas = datas
    """

    def insert(self, *columns):
        key = columns[0]
        cur_time = int(time.time())
        rid = key % 100000000  # we use rid as the current, so when ever it create, it is unique
        indirect = 0
        schema_encoding = '0' * self.table.num_columns
        record = Record(key, rid, indirect, schema_encoding,
                        cur_time, self.num_col, list(columns[1:]))
        self.num_col += 1
        schema_encoding = '0' * self.table.num_columns
        self.table.write(record)

    """ Select a record with specified columns"""

    def select(self, key, column, query_columns):
        data = self.find_data_by_key(key)

        if not LockManager.check_validation(updatekey, key):
            return False

        LockManager.read_phase_update(lockedkey, key, Lock)
        if 0 != data[5]:
            data = self.check_for_update(data[5])
        ret_data = [data[0]]
        ret_data = ret_data + data[6:]
        for record in self.update_list:
            if record.key == key:
                ret_data = [record.key]
                ret_data = ret_data + record.datas
        ret_record = Record_For_User(ret_data[0], ret_data[1], ret_data)
        list_data_a = [ret_record]

        # release the keys
        LockManager.read_phase_release(lockedkey, key, Lock)
        return list_data_a

    """ Update a record with specified key and columns """

    def update(self, key, *columns):
        if self.locked:
            self.merge_start()

        rid = self.table.key_to_rid[key]
        index = self.table.rid_to_index[rid]
        old_data = self.find_data_by_key(key)

        """ check for the validation first """
        if not LockManager.check_validation(lockedkey, key):
            return False
        """ the program goes here means it is valid to update """
        LockManager.read_phase_update(lockedkey, key, Lock)
        LockManager.read_phase_update(updatekey, key, Lock)

        if not self.locked:
            new_data = self.combine_old_data(old_data, *columns)
            new_record = Record(new_data[0], new_data[1], new_data[2],
                                new_data[3], new_data[4], new_data[5], new_data[6:])
            self.table.modify(key, new_record, index)
            self.table.write(new_record, TYPE.TAIL)
            #REDOLOG.write_record(new_record)
        else:
            print("i am in locked")
            new_data = self.combine_old_data(old_data, *columns)
            new_record = Record(new_data[0], new_data[1], new_data[2],
                                new_data[3], new_data[4], new_data[5], new_data[6:])
            new_record.basekey = old_data[0]
            self.update_list.append(new_record)
            self.table.write(new_record, TYPE.TAIL)

        self.locked = self.merge_count_down()
        LockManager.read_phase_release(updatekey, key, Lock)
        LockManager.read_phase_release(lockedkey, key, Lock)
        return True

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    """
        merge implementation
    """

    def merge_start(self):
        self.merge_lock = True

        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = {executor.submit(self.merge_process)}
            for task in concurrent.futures.as_completed(results):
                self.merge_data_update()
                self.merge_update()
                self.locked = False
                self.MERGE_COUNTER = MERGE_COUNTER
                self.update_list = []

    def merge_process(self):
        new_data_array = []
        for key in self.table.key_to_rid:
            data = self.find_data_by_key(key)
            origin_rid = data[1]  # origin rid
            if 0 != data[5]:
                data = self.check_for_update(data[5])
            new_data = [data[0]] + [origin_rid] + data[2:5] + [0]  # origin key and origin rid
            new_data = new_data + data[6:]  # origin key, rid with newest data
            new_data_array.append(new_data)

        new_pages_array = []
        pages_num_array = []
        for page_num in self.table.page_directory:
            if page_num < TAIL_PAGE_NUM:
                pages = []
                for i in range(self.table.num_columns + INTER_DATA_COL):
                    page = Page()
                    pages.append(page)
                new_pages = Pages(page_num, pages)
                new_pages_array.append(new_pages)
                pages_num_array.append(page_num)

        pages_counter = 0

        for i in range(len(new_data_array)):
            if new_pages_array[pages_counter].pages[0].has_capacity():
                for j in range(len(new_pages_array[pages_counter].pages)):
                    new_pages_array[pages_counter].pages[i].write(new_data_array[i][j])
            else:
                pages_counter += 1

        self.merged_page_num = pages_num_array
        self.merged_pages = new_pages_array
        self.merge_lock = False

    def merge_count_down(self):
        self.MERGE_COUNTER -= 1
        if self.MERGE_COUNTER <= 0:
            return True
        return False

    # self.key = key
    # self.rid = rid
    # self.columns = columns
    # self.schema = schema_encode
    # self.time = now
    # self.indirect = indirect
    # self.datas = datas

    def merge_update(self):
        for record in self.update_list:
            old_data = self.find_data_by_key(record.basekey)
            index = self.table.rid_to_index[old_data[1]]
            pages_id = self.table.page_directory[index.page_number]
            if record.key == old_data[0]:
                pages = self.table.buffer_manager.get_pages(pages_id)
                pages.pages[5].modify(index, record.rid)

    def merge_data_update(self):
        for i in range(len(self.merged_page_num)):
            pid = self.table.page_directory[self.merged_page_num[i]]
            self.table.buffer_manager.update(pid, self.merged_pages[i])

    """
        sum implementation
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        # sort the key in dictionary
        # sum them up by two range
        sum = 0
        sorted_keys = sorted(
            self.table.key_to_rid.items(), key=operator.itemgetter(0))
        on_add = False
        for i in range(end_range - start_range + 1):
            if start_range + i in self.table.key_to_rid:
                data = self.select(start_range + i, 0, [1, 1, 1, 1, 1])[0]
                sum += data.columns[aggregate_column_index]
        return sum

    """
        utilities implementation
    """

    def find_data_by_key(self, key):
        if key not in self.table.key_to_rid:
            print("can't find key")
            exit()
        rid = self.table.key_to_rid[key]
        page_dir = self.table.rid_to_index[rid]
        pages_id = self.table.page_directory[page_dir.page_number]
        pages = self.table.buffer_manager.get_pages(pages_id)
        data = []
        for i in range(len(pages.pages)):
            data.append(self.read_data(pages.pages[i], page_dir.start_index, page_dir.end_index))
        return data

    def read_data(self, page, start_index, end_index):
        # this is raw data, not handled
        data = page.read_data(start_index, end_index)
        ret_data = ''
        for i in range(len(data)):
            if data[i] < 10:
                ret_data += '0' + str(data[i])
            elif 255 == data[i]:
                break
            else:
                ret_data += str(data[i])
        return int(ret_data)

    def combine_old_data(self, old_data, *columns):
        self.update_counter += 1
        if old_data[5] != 0:
            old_data = self.check_for_update(old_data[5])
        old_data = self.check_in_update_list(old_data)
        filtered_data = old_data
        if columns[0] is not None:
            filtered_data[0] = columns[0]

        for i in range(len(columns) - 1):
            if columns[i + 1] is not None:
                filtered_data[6 + i] = columns[i + 1]
        filtered_data[1] = self.update_counter  # (randint(0,int(old_data[1])) + self.update_counter) % 33000
        return filtered_data

    def check_for_update(self, rid):
        if rid not in self.table.tail_index_lookup:
            print("can't find key in tail")
            exit()
        page_dir = self.table.tail_index_lookup[rid]
        pages_id = self.table.page_directory[page_dir.page_number]
        pages = self.table.buffer_manager.get_pages(pages_id)
        data = []
        for i in range(len(pages.pages)):
            data.append(self.read_data(pages.pages[i], page_dir.start_index, page_dir.end_index))
        return data

    def check_in_update_list(self, old_data):
        # (self, key, rid, indirect, schema_encode, now, columns, datas):
        if len(self.update_list) == 0:
            return old_data
        else:
            new_data = old_data
            for record in self.update_list:
                if old_data[0] == record.key:
                    new_data = [record.key, record.rid, record.indirect, record.schema, record.time, record.columns]
                    new_data = new_data + record.datas
            return new_data
