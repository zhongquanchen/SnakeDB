
# 1. have the base page and tail merge together
     #. need to run in background
     #. all the base page will copy to a new buffer pool

# 2. have new update tail record point to baseRID
     #. modify the base-merged record indirection to the new update tail record during merge

class merge :
    pass