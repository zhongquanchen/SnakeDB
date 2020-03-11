from lstore.src.index import Index
from lstore.src.table import *
from lstore.src.db import *

class Transaction:
    """
    # Creates a transaction object.
    """

    def __init__(self):
        self.queries = []
        self.db_transactions = []
        self.db_state = {}
        pass

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
                return self.abort()
        return self.commit()

    def abort(self):
        # TODO: do roll-back and any other necessary operations
        # Return false, and go back to where it was before the last begin in database, if there is a transaction
        # if self.db_transactions:
        #     for name, value in self.db_transactions:
        #         if value:
        #             self.db_state[name] = value
        #         else:
        #             self.db_state.pop(name)
        #     self.db_transactions.pop()
        # else:
        #     print('There is no transaction')
        return False

    def commit(self):
        # TODO: commit to database
        # Database.close = staticmethod(Database.close)
        # Database.close()
        return True
