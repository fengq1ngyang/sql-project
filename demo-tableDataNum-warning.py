# 通过自动化运行SQL，统计table_row_check中各表近期内丢失数据情况、数据不变天数
import psycopg2
import time

sql = """
with
    temp1 as (
                SELECT
                    *,
                    dense_rank() over (partition by "NAME" order by "ROWCOUNT" desc ) time_rank
                FROM
                    dw.table_rows_check
                WHERE
                    DATE_PART('day', now() - table_rows_check.create_time) <= 7
                    AND "NAME" not like 'dwd%'
    ),
    temp2_rowNum as (
        SELECT  "NAME",count(*) row_num
        FROM temp1
        group by "NAME"
    ),
    temp3_1num as(
        SELECT "NAME" ,count(*) count_1num
        FROM temp1
        where time_rank=1
        group by time_rank, "NAME"
        ),
    temp4_important_rows as (
        SELECT id,
               "ROWCOUNT",
               "NAME" ,
               time_rank ,
               min(create_time)over(partition by "NAME" order by create_time desc) create_time,
               table_id,
               table_comments
        from temp1
        WHERE "NAME" not in (
            SELECT t2."NAME"
            FROM
                temp2_rowNum t2 inner join temp3_1num t3 on t2."NAME" = t3."NAME"
            where
                ROUND(t3.count_1num / t2.row_num,2)  >=  0.9 --只要1的数量占90%以上就过滤掉
        )
    )
SELECT t4.id as id,
       table_id,
       t4."NAME" as table_name,
       "ROWCOUNT",
       today_less_value,
       today_more_value,
       is_equal_value,
      t4.time_rank,
      create_time
from temp4_important_rows t4 join u_hq.table_row_check_rules h1 on t4. "NAME" = h1.table_name
"""

# 从u_hq.table_row_check_warning更新到dw.table_rows_check 的 warning_type 字段
sql_finish = """
UPDATE dw.table_rows_check t1
SET warning_type = (
    case
    when t2.is_over_less > 0 then 'today_less'
    when t2.is_over_more > 0 then 'today_more'
    else concat('equal_',t2.is_equal_value)
    end
    )
FROM
    (SELECT *
    FROM u_hq.table_row_check_warning
    WHERE is_over_less > 0 or is_over_more > 0 or is_equal_value > 1
    ) t2
WHERE  t1."NAME" = t2.table_name AND  date_trunc('day', now()) =  date_trunc('day',t1.create_time);
"""

"""
   执行SQL语句，插入到列表
"""


def conn_executeSQL(sql_go):
    # 创建连接对象
    conn = psycopg2.connect(database="fsdrm", user="hehongquan", password="yhlz@123", host="172.16.0.127", port="5432")
    cur = conn.cursor()  # 创建指针对象

    # 获取结果
    cur.execute(sql_go)
    try:
        results = cur.fetchall()
    except Exception as e:
        print(e)
        results = ""
    # 关闭连接
    conn.commit()
    cur.close()
    conn.close()

    # 返回结果
    return results


def compute_list(data):
    result_list = []
    cache_list = [data[0]]
    for row in data[1:]:
        if row[2] == cache_list[0][2]:
            cache_list.append(row)
        else:
            # 计算
            # print(cache_list)

            is_equal_value = 0
            is_over_more = 0
            is_over_less = 0
            result_row = []

            first_row = cache_list[0]
            # print(first_row[2])
            for cache in cache_list[1:]:
                if first_row[3] == cache[3]:
                    is_equal_value += 1
                elif is_equal_value > 0 and first_row[3] != cache[3]:
                    break
                elif first_row[3] > cache[3]:
                    more = (first_row[3] - cache[3]) / cache[3] * 100
                    if more >= first_row[5]:
                        is_over_more = more
                    break
                elif first_row[3] < cache[3]:
                    less = (cache[3] - first_row[3]) / cache[3] * 100
                    if less >= first_row[4]:
                        is_over_less = less
                    break
            # 如果需要添加天数阈值在这里用if 判断，是否大于字段中的is_equal_value
            result_row = [first_row[0], first_row[1], first_row[2], is_equal_value, is_over_more, is_over_less,
                          first_row[8], first_row[3]]

            # 计算完清空cache_list
            cache_list = []

            # 清空完添加
            result_list.append(result_row)
            cache_list.append(row)
            pass

    return result_list


def update_table(data):
    # 创建连接对象
    conn = psycopg2.connect(database="fsdrm", user="hehongquan", password="yhlz@123", host="172.16.0.127", port="5432")
    cur = conn.cursor()  # 创建指针对象

    table_name = "u_hq.table_row_check_warning"

    # update_sql的操作是在表table中col_A=1同时col_B=2的行的col_C这一列的数据更新为3

    # 清空表
    clear_table_sql = "truncate table " + table_name
    cur.execute(clear_table_sql)
    # 再插入
    insert_sql = f"insert into {table_name} values(%s, %s, %s,%s, %s, %s,%s,%s)"
    cur.executemany(insert_sql, data)

    # 关闭连接
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':

    while True:
        print(time.strftime("%H:%M:%S", time.localtime()) + "   BEGIN" )

        table_list = conn_executeSQL(sql)

        result = compute_list(table_list)
        print(result)
        print(time.strftime("%H:%M:%S", time.localtime()) + "   compute")
        update_table(result)
        print(time.strftime("%H:%M:%S", time.localtime()) + "   insert")

        conn_executeSQL(sql_finish)
        print(time.strftime("%H:%M:%S", time.localtime()) + "   update")

        print(time.strftime("%H:%M:%S", time.localtime()) + "   END")
        print("======================================================")
        time.sleep(10800)

        # time_now = time.strftime("%H:%M:%S", time.localtime())  # 刷新
        # if time_now == "15:00:10":  # 此处设置每天定时的时间
        #     # 此处为需要执行的动作
        #     # 连接数据库，执行SQL查询
        #
        #     time.sleep(2)  # 因为以秒定时，所以暂停2秒，使之不会在1秒内执行多次
