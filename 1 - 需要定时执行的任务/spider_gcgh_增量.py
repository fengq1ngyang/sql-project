import threading
import time
import requests
import json
from bs4 import BeautifulSoup
import DBUtil
from multiprocessing import Process, Queue, Manager
import urllib3
from functools import partial
from concurrent.futures import ThreadPoolExecutor

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

"""
    将数据库 dw_temp.zrzy_sert_gcgh 表中的
"""

base_url = 'https://zsxx.fszrzy.foshan.gov.cn/GTGHService/home/SearchData'
output_file = 'C:\\Users\\元亨利贞\\Desktop\\增量-result.xlsx'
lock = threading.Lock()
pgUtil = DBUtil.PgUtil()
request_body = {
    "strWhere": '2023',
    "action": 'gcgh',
    "area": '',
    "pageIndex": 1,
    "pageSize": 10000,
}
insert_sql_10 = """insert into ods.zrzy_sert_gcgh(收件编号,证书编号,项目名称,单体名称,"建设单位(个人)",核发日期,建设位置,建设规模,附图及附件名称,sert_id) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
insert_sql_9 = """insert into ods.zrzy_sert_gcgh(收件编号,证书编号,项目名称,"建设单位(个人)",核发日期,建设位置,建设规模,附图及附件名称,sert_id) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

"""完成翻页，生成所有详情页url"""
def do_craw_url(detail_url: Queue, page_queue: Queue):
    global request_body
    while True:
        try:
            pageNum = page_queue.get(timeout=10)
        except:
            # print("do_craw_url退出")
            break
        request_body['pageIndex'] = pageNum
        # print(f"page={pageNum}")
        response = requests.post(url=base_url, data=request_body, verify=False)
        body = response.json()
        uuids = json.loads(body['datas'])
        for uuid in uuids:
            url_ = f"https://zsxx.fszrzy.foshan.gov.cn/GTGHService/ViewCase/gcghxkz/{uuid['5']}"
            detail_url.put(url_)


"""
从queue获取了详情页的url，判断是否在数据库里面
在就跳过，不在就下载该url并解析
"""
def do_parser(detail_url: Queue, DB_url_list, lst9, lst10):
    while True:
        try:
            # print(detail_url.qsize())
            url = detail_url.get(timeout=15)
        except:
            # print("do_parser退出")
            break
        if url in DB_url_list:
            pass
        else:
            html = requests.get(url=url, verify=False).text
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find('div', {'id': 'yslz-info'}).find('table')
            fields = []
            for row in table.find_all('tr')[1:]:
                value = None
                if row.find('td') is not None:
                    value = row.find('td').text.strip()
                fields.append(value)
            fields.append(url.split('/')[-1])
            if len(fields) == 9:
                lst9.append(fields)
            else:
                lst10.append(fields)


"""查询DB_url_list：返回数据库已存在url列表"""
def select_url_list():
    sql_code = f"""
        select concat('https://zsxx.fszrzy.foshan.gov.cn/GTGHService/ViewCase/gcghxkz/',sert_id)
        from ods.zrzy_sert_gcgh
        where "证书编号" like '%{request_body['strWhere']}%'
    """
    data = pgUtil.get_data(sql_code)
    new_list = list(map(lambda x: x[0], data))
    return new_list


"""获取总页数"""
def get_pageNum():
    global request_body
    response = requests.post(url=base_url, data=request_body, verify=False)
    body = response.json()
    page_count = body['pageCount']
    count = body['count']
    print("总页数：", page_count,"总记录数：", count)
    return int(page_count)


# 插入数据
def insert_table_ft(insert_sql, data):
    obj = DBUtil.PgUtil()
    obj.insert_table(insert_sql, data)


# main
def main():
    start = time.time()

    # 共享管道
    detail_url = Queue()
    page_queue = Queue()
    # 共享内存列表
    manager = Manager()
    lst9 = manager.list()
    lst10 = manager.list()

    DB_url_list = select_url_list()

    # 将页数添加到 pipeline
    for index in range(1, get_pageNum() + 1):
        page_queue.put(index)

    # 启动生产者，获取每页中sert_id拼接成详情页url添加到 pipeline
    t1_thread = []
    for idx in range(1):
        t1_thread.append(threading.Thread(target=do_craw_url, args=(detail_url, page_queue,)))
    # 启动消费者，获取详情页信息，将数据添加到 共享内存列表
    t2_process = []
    for idx in range(1):
        t2_process.append(threading.Thread(target=do_parser, args=(detail_url, DB_url_list, lst9, lst10)))

    # 线、进程的开启和等待结束
    for thread in t1_thread:
        thread.start()
    for thread in t2_process:
        thread.start()
    for thread in t1_thread:
        thread.join()
    for thread in t2_process:
        thread.join()

    print("新增数量：", len(lst9) + len(lst10))

    # 数据分组，500 一组
    m9 = []
    for index in range(0, len(lst9), 500):
        m9.append(lst9[index:index + 500])

    m10 = []
    for index in range(0, len(lst10), 500):
        m10.append(lst10[index:index + 500])

    # 启动线程池
    with ThreadPoolExecutor() as pool:
        pool.map(partial(insert_table_ft, insert_sql_9), m9)

    with ThreadPoolExecutor() as pool:
        pool.map(partial(insert_table_ft, insert_sql_10), m10)

    end = time.time()
    print("耗时: ", end - start)


if __name__ == '__main__':

    main()
    # while True:
    #     time_now = time.strftime("%H", time.localtime())  # 刷新
    #     print("Now Time：", time.strftime("%H:%M:%S", time.localtime()))
    #     if time_now == "10":  # 此处设置每天定时的时间
    #         # 此处为需要执行的动作
    #         try:
    #             main()
    #         except:  # 失败重试（主要是远程数据库网络问题）
    #             time.sleep(5)
    #             main()
    #     time.sleep(3500)  # 因为以秒定时，所以暂停2秒，使之不会在1秒内执行多次
