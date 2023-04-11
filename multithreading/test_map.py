from concurrent.futures import ThreadPoolExecutor
from functools import partial

def insert_table(sql,data):
    print(sql,data)
    return 1

if __name__ == '__main__':
    list = [1,2,3,4,5,6,7,8]
    sql = 'ai'

    list_2 = [
        [1,2],[1,2],[1,2],[1,2],[1,2],[1,2],[1,2]
    ]
    arr = [[row[1]] for row in list_2]
    print(arr)
    # m = []
    # for index in range(0,len(list),3):
    #     m.append(list[index:index+3])
    # print(m)
    # # with ThreadPoolExecutor() as pool:
    # #     pool.map(partial(insert_table,sql),list)