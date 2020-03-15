from lstore.src.index import Index
from lstore.src.query import Query
from lstore.src.table import *
from lstore.src.db import *
from lstore.src.redoLog import *


class Transaction:
    """
    # Creates a transaction object.
    """

    def __init__(self):
        self.queries = []
        self.db_transactions = []
        self.db_state = {}
        self.keys = []

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False
    # on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if not result:
                print("start aborting")
                return self.abort()
            else:
                self.keys.append(args[0])
        return self.commit()

    def abort(self):
        # TODO: do roll-back and any other necessary operations
        with manager_lock:
            records = REDOLOG.roll_back_action(self.keys)
        return False

    def commit(self):
        self.keys.clear()
        # TODO: commit to database
        with manager_lock:
            REDOLOG.clearList()
        return True
