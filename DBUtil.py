import time
import numpy as np
import psycopg2


class PgUtil:
    def __init__(self):
        # 创建连接对象
        self.conn = None
        self.cur = None
        self.database = "fsdrm"
        self.user = "hehongquan"
        self.password = "yhlz@123"
        self.host = "172.16.0.127"
        self.port = "5432"

    def get_conn(self):
        """ 获取conn """
        self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host,
                                     port=self.port)
        self.cur = self.conn.cursor()  # 创建指针对象

    def close(self):
        """ 关闭连接 """
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def get_data(self, sql):
        """ 查询数据 """
        res = None
        try:
            self.get_conn()
            self.cur.execute(sql)
            res = self.cur.fetchall()
        except Exception as e:
            print(e)
            pass
        self.close()
        return res

    def insert_table(self, sql, data):
        """ 插入数据 """
        # executemany 必须为二维数组
        for _ in range(10):
            try:
                self.get_conn()
                self.cur.executemany(sql, data)
                print(" -- 成功插入 {} 条 -- ".format(len(data)))
                break
            except Exception as exc:
                print(exc)
                time.sleep(2)
        self.close()

    def truncate_table(self, table_name):
        """清空表"""
        self.get_conn()
        clear_table_sql = "truncate table " + table_name
        self.cur.execute(clear_table_sql)
        self.close()
