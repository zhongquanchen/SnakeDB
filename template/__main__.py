from template.db import Database
from template.query import Query
from time import process_time
from random import choice, randrange

# # Student Id and 4 grades
# db = Database()
# grades_table = db.create_table('Grades', 5, 0)
# query = Query(grades_table)
# keys = []
#
# # Measuring Insert Performance
# insert_time_0 = process_time()
# for i in range(0, 10000):
#     query.insert(906659671 + i, 93, 0, 0, 0)
#     keys.append(906659671 + i)
# insert_time_1 = process_time()
# print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)
#
# # Measuring update Performance
# update_cols = [
#     [randrange(0, 100), None, None, None, None],
#     [None, randrange(0, 100), None, None, None],
#     [None, None, randrange(0, 100), None, None],
#     [None, None, None, randrange(0, 100), None],
#     [None, None, None, None, randrange(0, 100)],
# ]
#
# update_time_0 = process_time()
# for i in range(0, 10000):
#     query.update(choice(keys), *(choice(update_cols)))
# update_time_1 = process_time()
# print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)
#
# # print(query.table.page_directory)
# # print(query.table.page_directory[196].data)
# # Measuring Select Performance
# select_time_0 = process_time()
# for i in range(0, 10000):
#     data = query.select(choice(keys), [1, 1, 1, 1, 1])
#     # for i in range(len(data)):
#     #     print(data[i], end='')
#     # print()
#     # print()
# select_time_1 = process_time()
# print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)
#
# Measuring Aggregate Performance
# agg_time_0 = process_time()
# for i in range(0, 10000, 100):
#     result = query.sum(i, 100, randrange(0, 5))
# agg_time_1 = process_time()
# print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)
#
# # Measuring Delete Performance
# delete_time_0 = process_time()
# for i in range(0, 10000):
#     query.delete(906659671 + i)
# delete_time_1 = process_time()
# print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

# Measuring Insert Performance
insert_time_0 = process_time()
for i in range(0, 10000):
    query.insert(906659671 + i, 93, 0, 0, 0)
    keys.append(906659671 + i)
insert_time_1 = process_time()
print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

page = query.table.page_directory[0]

# Measuring Select Performance
select_time_0 = process_time()
for i in range(0, 10000):
    data = query.select(choice(keys), [1, 1, 1, 1, 1])
    print(data)
select_time_1 = process_time()
print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)

# Measuring update Performance
update_cols = [
    [randrange(0, 100), None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

update = 0
update_time_0 = process_time()
for i in range(0, 10000):
    update = choice(keys)
    columns = choice(update_cols)
    print("update cols ", columns, "update key ", update)
    query.update(update, columns[0], columns[1],
                 columns[2], columns[3], columns[4])
    print(query.select(update, [1, 1, 1, 1, 1]))
    print()
update_time_1 = process_time()
print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)

print()
print()
print()
cols = [1, 10, 1000, 10000, 100000]
query.update(906667265, cols[0], cols[1], cols[2], cols[3], cols[4])
print(query.select(906667265, [1, 1, 1, 1, 1]))

query.delete(906667265)
query.insert(906667265, 93, 0, 0, 0)
print(query.select(906667265, [1, 1, 1, 1, 1]))
