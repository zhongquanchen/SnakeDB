"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""

"""
# will have page number, record start index, and record end index
"""


class Index:

    def __init__(self, *indexes):
        self.page_number = indexes[0]
        self.start_index = indexes[1]
        self.end_index = indexes[2]
