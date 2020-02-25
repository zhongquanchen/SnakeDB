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

    """
    # returns the location of all records with the given value
    """

    def locate(self, value):

        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, table, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, table, column_number):
        pass
