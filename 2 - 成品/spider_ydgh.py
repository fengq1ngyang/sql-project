import threading
import time
import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import DBUtil
from multiprocessing import Process,Queue,Manager
from concurrent.futures import ThreadPoolExecutor
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

"""
    将数据库 dw_temp.zrzy_sert_gcgh 表中的
"""

base_url = 'https://zsxx.fszrzy.foshan.gov.cn/GTGHService/home/SearchData'
output_file = 'C:\\Users\\元亨利贞\\Desktop\\增量-result.xlsx'
lock = threading.Lock()
pgUtil = DBUtil.PgUtil()
request_body = {
    "strWhere": '',
    "action": 'ydgh',
    "area": '',
    "pageIndex": 1,
    "pageSize": 10000,
}
insert_sql = """insert into ods.zrzy_sert_ydgh(收件编号,证书编号,项目名称,建设单位,核发日期,用地位置,用地面积（平方米）,用地性质,建设规模,附图及附件名称,sert_id) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

"""完成翻页，生成所有详情页url"""
def do_craw_url(detail_url:Queue,page_queue:Queue):
    global request_body
    while True:
        try:
            pageNum = page_queue.get(timeout=10)
        except:
            print("do_parser退出")
            break
        request_body['pageIndex'] = pageNum
        print(f"page={pageNum}")
        response = requests.post(url=base_url, data=request_body, verify=False)
        body = response.json()
        uuids = json.loads(body['datas'])
        for uuid in uuids:
            url_ = f"https://zsxx.fszrzy.foshan.gov.cn/GTGHService/ViewCase/ydghxkz/{uuid['4']}"
            detail_url.put(url_)


"""
从queue获取了详情页的url，判断是否在数据库里面
在就跳过，不在就下载该url并解析
"""
def do_parser(detail_url:Queue,DB_url_list,lst):
    while True:
        try:
            print(detail_url.qsize())
            url = detail_url.get(timeout=10)
        except:
            print("do_parser退出")
            break
        if url in DB_url_list:
            pass
        else:
            html = requests.get(url=url,verify=False).text
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find('div', {'id': 'yslz-info'}).find('table')
            fields = []
            for row in table.find_all('tr')[1:]:
                value = None
                if row.find('td') is not None:
                    value = row.find('td').text.strip()
                fields.append(value)
            fields.append(url.split('/')[-1])
            lst.append(fields)

            # lst.append(fields)

"""查询DB_url_list"""
def select_url_list():
    sql_code = """
        select concat('https://zsxx.fszrzy.foshan.gov.cn/GTGHService/ViewCase/ydghxkz/',sert_id)
        from ods.zrzy_sert_ydgh
        -- where sert_code like '______2023_____'
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
    return int(page_count)

# 插入数据
def insert_table_ft(insert_sql,data):
    obj = DBUtil.PgUtil()
    obj.insert_table(insert_sql,data)

if __name__ == '__main__':
    start = time.time()

    # 共享缓存
    detail_url = Queue()
    page_queue = Queue()
    #全局列表
    manager = Manager()
    lst = manager.list()

    DB_url_list = select_url_list()

    for index in range(1,get_pageNum()+1):
        page_queue.put(index)

    t1_thread = []
    for idx in range(1):
        t1_thread.append(threading.Thread(target=do_craw_url,args=(detail_url,page_queue,)))
    t2_process = []
    for idx in range(50):
        t2_process.append(threading.Thread(target=do_parser, args=(detail_url, DB_url_list,lst)))

    #线程
    for thread in t1_thread:
        thread.start()
    #进程
    for thread in t2_process:
        thread.start()

    for thread in t1_thread:
        thread.join()
    for thread in t2_process:
        thread.join()

    t3_thread = []
    for index in range(0,len(lst),200):
        t = threading.Thread(target=insert_table_ft,args=(insert_sql,lst[index:index+200],))
        t3_thread.append(t)

    for thread in t3_thread:
        thread.start()
    for thread in t3_thread:
        thread.join()


    end = time.time()
    print("耗时:",end-start)
