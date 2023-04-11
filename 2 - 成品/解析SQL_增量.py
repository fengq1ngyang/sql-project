import json
import re
import time
from sqllineage.runner import *
import sqlparse
import DBUtil
from tqdm import tqdm
import datetime
import threading

"""
    1.查询表public.t_exec_log
    2.解析SQL
    3.插入表u_hq.sql_parse
"""


def read_sql():
    sql_code = """
select *
from public.t_exec_log
where sql_status = '成功' and sql_code is not null and md5(sql_code) not in (
    select sql_md5 from u_hq.sql_parse_middle where sql_md5 is not null
)
"""

    time_ = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    file_url = '{}-error.json'.format(time_)
    with open(file_url, 'a') as f:
        f.write("`# coding=UTF-8`\n")

    insert_table_name = 'u_hq.sql_parse'

    pg_util = DBUtil.PgUtil()

    data = pg_util.get_data(sql_code)
    print('*' * 5 + ' 数据条数： {}'.format(len(data)) + '*' * 5)
    data_list = []
    error_list = []
    for row in tqdm(data):
        # 将长sql切分为单个语句
        if row[9] is not None:
            sql_list = sqlparse.split(row[9])
            for index, item in enumerate(sql_list):
                # 太复杂的SQL会解析不出,将SQL信息保存起来，人工解析
                try:
                    item = sqlparse.format(item, reindent=True, keyword_case='upper', strip_comments=True)
                    result = LineageRunner(item, verbose=True)
                    if len(result.source_tables) > 0 and len(result.target_tables) > 0:
                        for source in result.source_tables:
                            match_obj = re.search(r'<default>', f"{source.schema}.{source.raw_name}")
                            if match_obj:
                                pass
                            else:
                                for target in result.target_tables:
                                    source_table = f"{source.schema}.{source.raw_name}"
                                    source_model = f"{source.schema}"
                                    target_table = f"{target.schema}.{target.raw_name}"
                                    target_model = f"{target.schema}"
                                    function_name = row[1]
                                    work_step = row[3]
                                    work_id = row[2]
                                    execution_time = row[6]
                                    sql_row_number = index + 1
                                    sql_code = str(f"{item}").strip().replace("'", "''")
                                    data_obj = [source_table, source_model, target_table, target_model, function_name,
                                                work_step, sql_code, sql_row_number, work_id,execution_time]
                                    data_list.append(data_obj)
                except Exception as e:
                    file_url = '{}-error.json'.format(time_)
                    error_list.append([row[1], row[3], str(f"{item}").strip().replace("'", "''"), index + 1, row[2], str(e),row[6]])
                    error_obj = {"function_name": row[1], "work_step": row[3], "error": str(e),
                                 "sql_code": str(f"{index + 1}.{item}").strip().replace("'", "''")}
                    with open(file_url, 'a') as f:
                        f.write(json.dumps(error_obj))
                        f.write("\n")

    # print('*' * 5 + ' 解析完成，删除原数据 ' + '*' * 5)
    # pg_util.truncate_table(insert_table_name)
    print('*' * 5 + ' 插入新数据 ' + '*' * 5)

    insert_sql = f"INSERT INTO {insert_table_name} (source_table, source_model, target_table, target_model, function_name,work_step, sql_code, sql_row_number, work_id,execution_time) " \
                 f"values (%s,%s, %s, %s,%s, %s, %s,%s,%s,%s)"
    insert_zl = """insert into sql_parse_middle (sql_md5) values (%s)"""
    # 分批插入
    for num in range(0,len(data_list),500):
        print(num)
        pg_util.insert_table(insert_sql, data_list[num:num + 500])
        arr = [(row[6]) for row in data_list[num:num + 500]]
        pg_util.insert_table(insert_zl,arr)

    # 插入解析失败SQL数据
    insert_error = f"INSERT INTO {insert_table_name} (function_name,work_step, sql_code, sql_row_number, work_id,error_msg,execution_time)" \
                   f"values (%s,%s, %s, %s,%s, %s,%s)"
    pg_util.insert_table(insert_error, error_list)

    print('*' * 5 + ' 数据插入完成 ' + '*' * 5)


if __name__ == '__main__':
    read_sql()
    # while True:
    #     print(time.strftime("%H:%M:%S", time.localtime()) + "   BEGIN" )
    #     # read_sql()
    #     print('*' * 5 + ' 程序执行结束 ' + '*' * 5)
    #     print(time.strftime("%H:%M:%S", time.localtime()) + "   END")
    #     print("======================================================")
    #     time.sleep(10800)