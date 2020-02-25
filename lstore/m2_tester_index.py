from lstore.src.db import Database
from lstore.src.Indexing import *

from random import choice, randint, sample, seed

db = Database()
db.open('~/ECS165')
# Student Id and 4 grades
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}
seed(3562901)
for i in range(0, 1000):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
keys = sorted(list(records.keys()))
print("Insert finished")

for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    # else:
    #     print('select on', key, ':', record)
print("Select finished")

db.close()
grades_table = db.get_table('Grades')

index = Indexing(grades_table)
ret_index = index.locate(1, 10)
print(ret_index)
# for j in range(len(ret_index[0])):
#     for i in range(len(ret_index)):
#         print(ret_index[i], end='')
#     print()
print("Indexing finished")
