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
            self.keys.append(args[0])
            if not result:
                return self.abort()
        return self.commit()

    def abort(self):
        # TODO: do roll-back and any other necessary operations
        with manager_lock:
            records = REDOLOG.roll_back_action(self.keys)
        return False

    def commit(self):
        with manager_lock:
            REDOLOG.commit(self.keys)
        self.keys.clear()
        return True
