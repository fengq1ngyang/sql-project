import re
import threading
import time
from sqllineage.runner import *
import sqlparse
import DBUtil
import logging
from multiprocessing import Queue,Lock
from  multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import atexit

# 注册全局线程池
pool = ThreadPoolExecutor()
DATA_ARRAY = []
lock = Lock()

pg_util = DBUtil.PgUtil()

insert_sql = f"INSERT INTO u_hq.sql_parse (source_table, source_model, target_table, target_model, function_name,work_step, sql_code, sql_row_number, work_id,error_msg,execution_time) " \
             f"values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

sql_code = """
with temp1 as (
select function_name,
        max(begin_time) as last_action
from public.t_exec_log
group by  function_name
)
select
	*,
	row_number() over (partition by  c.function_name order by id)
from(
    select
        t1.*,
        row_number() over(partition by t1.function_name,work_id order by t1.begin_time desc nulls last ) as rn
    from
        public.t_exec_log t1
    where
        sql_status = '成功'  --and function_name = 'dw.p_dwm_sgxkz_jbxx'
)
as c left join temp1 t1 on c.function_name = t1.function_name
where rn = 1 and date_trunc('day', t1.last_action) =  date_trunc('day',c.begin_time);
"""

"""执行SQL，获取数据"""
def pg_getData(row_Mqueue:Queue):
    pg_util = DBUtil.PgUtil()
    data = pg_util.get_data(sql_code)
    for row in data:
        #row元组
        row_Mqueue.put(row)
    print("******************* 发送数据完成 ******************")

"""解析"""
def sql_parse(row_Mqueue:Queue,result_Tqueue:Queue):
    global DATA_ARRAY
    while True:
        print("******************* row_Mqueue: {} ******************".format(row_Mqueue.qsize()))
        try:
            row = row_Mqueue.get(timeout=20)
        except:
            print("sql_parse退出")
            break
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
                                                work_step, sql_code, sql_row_number, work_id,None,execution_time]
                                    DATA_ARRAY.append(data_obj) #5309
                                    if len(DATA_ARRAY) >= 100:
                                        insert_table()

                except Exception as e:
                    error = [None, None, None, None, row[1], row[3], str(f"{item}").strip().replace("'", "''"), index + 1, row[2], str(e),row[6]]
                    DATA_ARRAY.append(error)
    insert_table()



"""写入数据库"""
def insert_table():
    with lock:
        global DATA_ARRAY
        pg_util.insert_table(insert_sql, DATA_ARRAY)
        DATA_ARRAY[:] = []

"""清空表"""
def tun_table():
    pg_util.truncate_table('u_hq.sql_parse')
    logging.info("******************* 清空原数据完成 ******************")


"""
    先从pgSQL查询数据，然后for循环放到row_queue空间里面，等待解析
    通过 sql_parse(row_queue,result_queue)读取row_queue中的数据，解析后写入到result_queue (CPU密集型任务) 多进程 
    最后通过 inset_table(result_queue,conn)读取result_queue中的数据，#并且通过传入的conn取消共用连接，他这个连接不稳定，回中断，所以还是每个进程创建单独的连接#
    pgSQL连接写入数据库 (IO密集型任务) 多线程 用线程池,在多线程里面用线程池submit提交任务
"""

def exit_handler():
    # 终止所有线程
    for thread in threading.enumerate():
        if thread is not threading.current_thread():
            thread.join()

    # 终止所有进程
    for process in multiprocessing.active_children():
        process.terminate()


if __name__ == '__main__':

    # 此处为需要执行的动作
    atexit.register(exit_handler)

    # 设置日志级别为
    logging.basicConfig(level=logging.INFO)
    # 清空原表
    tun_table()
    # 创建queue
    row_Mqueue = Queue()
    result_Tqueue = Queue()

    # 开启读取数据库的线程
    t = threading.Thread(target=pg_getData, args=(row_Mqueue,))
    t.start()

    # 开启解析的进程
    p_array = []
    for idx in range(8):
        p = Process(target=sql_parse, args=(row_Mqueue, result_Tqueue))
        p_array.append(p)
        p.start()

    t.join()
    for process in p_array:
        process.join()


    # while True:
    #     time_now = time.strftime("%H", time.localtime())  # 刷新
    #     print("Now Time：", time.strftime("%H:%M:%S", time.localtime()))
    #     if time_now == "12":  # 此处设置每天定时的时间
    #         # 此处为需要执行的动作
    #         atexit.register(exit_handler)
    #
    #         # 设置日志级别为
    #         logging.basicConfig(level=logging.INFO)
    #         # 清空原表
    #         tun_table()
    #         # 创建queue
    #         row_Mqueue = Queue()
    #         result_Tqueue = Queue()
    #
    #         # 开启读取数据库的线程
    #         t = threading.Thread(target=pg_getData, args=(row_Mqueue,))
    #         t.start()
    #
    #         # 开启解析的进程
    #         for idx in range(8):
    #             p = Process(target=sql_parse, args=(row_Mqueue, result_Tqueue))
    #             p.start()
    #     time.sleep(3500)  # 因为以秒定时，所以暂停2秒，使之不会在1秒内执行多次


