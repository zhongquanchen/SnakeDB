class Lock:
    def __init__(self):
        self.lockedRID = []
        self.locked = 0
        self.dictLocks = dict()

    def acquireLock(self, rid):
        if self.locked == 1:
            return false
        
        if not rid in self.lockedRID:
            self.lockedRID.append(rid)
            dictLocks[str(rid)] = 1
        else:
            dictLocks[str(rid)] += 1

        return true

    def releaseLock(self, rid):
        if self.locked == 1:
            return false
        
        if not rid in self.lockedRID:
            return true
        else:
            dictLocks[str(rid)] -= 1
            if dictLocks[str(rid)] == 0:
                self.lockedRID.remove(rid)
        
        return true

    def checkLock(self, rid):
        if not rid in self.lockedRID:
            return false
        else:
            return true

    def acquireLockManager(self):
        if locked == 0:
            locked = 1
        else:
            return true

    def releaseLockManager(self):
        if locked == 1:
            locked = 0
        else:
            return true

    def checkLockManager(self):
        if locked == 1:
            return true
        else:
            return false
        
