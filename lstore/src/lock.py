
"""
    The lock will implement validation concurrency control
"""

lockedkey = {}
updatekey = {}

class LockManager:
    def __init__(self):
        pass

    def read_phase_update(lockedkey, key):
        lockedkey.update({key: True})

    def read_phase_release(lockedkey, key):
        lockedkey.update({key: False})

    def write_phase_update(updatekey, key):
        updatekey.update({key: True})

    def write_phase_release(updatekey, key):
        updatekey.update({key: False})

    def check_update_valid(updatekey, key):
        if key in updatekey:
            # false means the key is not in locking status
            if updatekey[key] is False :
                return True
            else:
                return False
        return True

    def check_validation(lockedkey, key):
        if key in lockedkey:
            # false means the key is not in locking status
            if lockedkey[key] is False :
                return True
            else:
                return False
        return True

# class Lock:
#     def __init__(self):
#         self.lockedkey = {}
#         self.locked = 0
#         self.dictLocks = dict()
#
#     def acquireLock(self, rid):
#         if self.locked == 1:
#             return False
#
#         if not rid in self.lockedRID:
#             self.lockedRID.append(rid)
#             self.dictLocks[str(rid)] = 1
#         else:
#             self.dictLocks[str(rid)] += 1
#
#         return True
#
#     def releaseLock(self, rid):
#         if self.locked == 1:
#             return False
#
#         if not rid in self.lockedRID:
#             return True
#         else:
#             self.dictLocks[str(rid)] -= 1
#             if self.dictLocks[str(rid)] == 0:
#                 self.lockedRID.remove(rid)
#
#         return True
#
#     def checkLock(self, rid):
#         if not rid in self.lockedRID:
#             return False
#         else:
#             return True
#
#     def acquireLockManager(self):
#         if self.locked == 0:
#             locked = 1
#         else:
#             return True
#
#     def releaseLockManager(self):
#         if self.locked == 1:
#             locked = 0
#         else:
#             return True
#
#     def checkLockManager(self):
#         if self.locked == 1:
#             return True
#         else:
#             return False
#
