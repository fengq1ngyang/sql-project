import DBUtil

# 创建 DBUtil 实例
pg = DBUtil.PgUtil()

# 定义插入语句
insert_sql = "INSERT INTO u_hq.test_volatile_columns (column1, column2, column3, column4, column5, column6, column7) VALUES (%s, %s, %s, %s, %s, %s, %s)"

# 定义数据
data = [
    {'column1': 'value1', 'column2': 'value2', 'column3': 'value3'},
    {'column1': 'value4', 'column2': 'value5', 'column4': 'value6', 'column5': 'value7'}
]

# 将数据转换为元组格式
values = [(d.get('column1', ''), d.get('column2', ''), d.get('column3', ''), d.get('column4', ''), d.get('column5', ''), d.get('column6', ''), d.get('column7', '')) for d in data]

print(values)

# 插入数据到数据库
# pg.insert_table(insert_sql, values)
