
"""
    The lock will implement validation concurrency control
"""

lockedkey = {}
updatekey = {}
Lock = False

class LockManager:
    def __init__(self):
        pass

    def read_phase_update(lock, key, Lock):
        if Lock:
            return False
        Lock = True

        value = 1
        if key in lock:
            value = lock[key]
            value += 1
        lock.update({key: value})
        Lock = False
        return True

    def read_phase_release(lock, key, Lock):
        if Lock:
            return False
        Lock = True

        value = 0
        if key in lock:
            value = lock[key]
            value -= 1
        else :
            print("value is not assign with this key in lockedkeys: ", key)
        lock.update({key: value})
        Lock = False
        return True

    def check_validation(lock, key):
        if key in lock:
            # false means the key is not in locking status
            if lock[key] == 0:
                return True
            else:
                return False
        return True


"""
    Global Interpreter Locks
"""
LockManger = LockManager()

