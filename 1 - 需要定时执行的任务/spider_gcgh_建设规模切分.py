import threading
import time
import requests
import json
from bs4 import BeautifulSoup
import DBUtil
from multiprocessing import Queue, Manager
import urllib3
from functools import partial
from concurrent.futures import ThreadPoolExecutor
import re
import cn2an

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


base_url = 'https://zsxx.fszrzy.foshan.gov.cn/GTGHService/home/SearchData'
lock = threading.Lock()
pgUtil = DBUtil.PgUtil()
request_body = {
    "strWhere": '2023',
    "action": 'gcgh',
    "area": '',
    "pageIndex": 1,
    "pageSize": 11500,
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Referer': 'https://zsxx.fszrzy.foshan.gov.cn/GTGHService/Home/ZSSearch/',
    'Upgrade-Insecure-Requests': '1',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Origin': 'https://zsxx.fszrzy.foshan.gov.cn',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache'
}

proxy_all = {
    'https': 'http://47.113.147.115:7888'
}


insert_sql_10 = """
    INSERT INTO ods.zrzy_sert_gcgh (sert_id, 收件编号, 证书编号, 项目名称, 单体名称, "建设单位(个人)", 核发日期, 建设位置, 建设规模, 附图及附件名称, 建筑面积, 用地面积, 计容面积, 不计容面积, 基底面积, 地上建筑面积, 地下建筑面积, 建筑高度, 地下层数, 地上层数, 管线长, 管径)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

# 完成翻页，生成所有详情页url
def do_craw_url(detail_url: Queue, page_queue: Queue):
    global request_body
    global proxy_all
    while True:
        try:
            pageNum = page_queue.get(timeout=10)
        except:
            # print("do_craw_url退出")
            break
        request_body['pageIndex'] = pageNum
        response = requests.post(url=base_url, data=request_body, verify=False)
        body = response.json()
        uuids = json.loads(body['datas'])
        for uuid in uuids:
            url_ = f"https://zsxx.fszrzy.foshan.gov.cn/GTGHService/ViewCase/gcghxkz/{uuid['5']}"
            detail_url.put(url_)



# 完成解析
def do_parser(detail_url: Queue, DB_url_list, lst10):
    # 从queue获取了详情页的url，判断是否在数据库里面
    # 在就跳过，不在就下载该url并解析
    global proxy_all
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
            html = requests.get(url=url, headers=headers,verify=False).text
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find('div', {'id': 'yslz-info'}).find('table')
            fields = {'收件编号':None,'证书编号':None,'项目名称':None,'单体名称':None,'建设单位(个人)':None,'核发日期':None,'建设位置':None,'建设规模':None,'附图及附件名称':None,'sert_id':None,}
            jsgm_obj = []
            for index,row in enumerate(table.find_all('tr')[1:]):
                key = row.find('th').text.strip()  # strip方法去除任何前导或尾随空格
                if key in fields.keys():
                    if row.find('td') is not None:
                        value = row.find('td').text.strip()
                    # 识别出建设规模字段,调用切分建设规模的函数，按照一定的顺序返回切分好的数组，
                        fields[key] = value
                    if key == '建设规模':
                        jsgm_obj = split_jzgm(value)
            fields['sert_id'] =url.split('/')[-1]
            # 建设规模切分
            fields = [ value for key,value in fields.items()]

            fields += jsgm_obj
            if len(fields) != 20:
                print(fields)
            lst10.append(fields)


# 查询DB_url_list：返回数据库已存在url列表
def select_url_list():
    sql_code = f"""
        select concat('https://zsxx.fszrzy.foshan.gov.cn/GTGHService/ViewCase/gcghxkz/',sert_id)
        from ods.zrzy_sert_gcgh 
        where "证书编号" like '%{request_body['strWhere']}%'
    """
    data = pgUtil.get_data(sql_code)
    new_list = list(map(lambda x: x[0], data))
    print("库中数量：",len(new_list))
    return new_list


# 获取总页数
def get_pageNum():
    global request_body
    global proxy_all
    response = requests.post(url=base_url, data=request_body, headers=headers,verify=False)
    body = response.json()
    page_count = body['pageCount']
    count = body['count']
    print("总页数：", page_count)
    print("总记录数：", count)
    return int(page_count)

# 切分建筑规模
def split_jzgm(str):

    row = {}

    str = str.replace("：", ":").replace("，", ",").replace(" ", "").replace("；", ";").replace(";", ',').replace('（',',').replace('）', ',').split('。')[0].split(",")

    pattern = r'(\S+):([\S+\.㎡层平方米]+)'

    # 新的字典确保顺序，以及完整性
    new_row = {'建筑面积': None, '用地面积': None, '计容面积': None, '不计容面积': None, '基底面积': None,
               '地上建筑面积': None,
               '地下建筑面积': None, '建筑高度': None, '地下层数': None, '地上层数': None,
               '管线长': None, '管径': None}

    # 判断是否为纯数字


    # Find all matches in the string
    for s in str:
        is_num = False
        num = ''
        try:
            num = float(s)
            is_num = True
        except ValueError:
            pass

        # 判断是否为 管线长:xxxx;管径为xxx类型数据
        if '管线' in s :
            re_ = r"(\d+)(\.\d+)?"
            reso_arr = re.findall(re_,s)
            for item in reso_arr:
                new_row['管线长'] = ''.join(list(item))
        elif '管径' in s:
            s = s.replace(":",'')
            re_date = re.findall(r"([\u4e00-\u9fa5])", s)
            for key in re_date:
                s = s.replace(key, '')
            new_row['管径'] = s
            pass
        # 判断是否包含数字
        elif is_num:
            new_row['建筑面积'] = num
        else:
            matches = re.findall(pattern, s)
            for key, value in matches:
                # 如果是长度超过6 而且存在顿号就默认为 value是这种类型 建筑层数：地上1层、30层、32层、地下2层
                if (len(value) > 6 and "、" in value) or "、" in value:
                    row[key] = value

                elif re.match(r"地(\S{,4})层", value):
                    try:
                        match = re.match(r"地(\S{,4})层", value)
                        row[key] = int(cn2an.cn2an(match.group(1)[1:], "smart"))
                    except:
                        # print(str)
                        pass
                else:
                    if "." in value:
                        match = re.search(r"(\d+\.\d+)(\D+)", value)
                    else:
                        match = re.search(r"(\d+)(\D+)", value)
                    if match:
                        try:
                            num = int(match.group(1))
                        except:
                            num = float(match.group(1))
                        if num <= 0:
                            pass
                        else:
                            row[key] = match.group(1)
                    else:
                        if value[0].isdigit():
                            try:
                                num = int(value)
                            except:
                                num = float(value)
                            if num <= 0:
                                pass
                            else:
                                row[key] = value

    for key, value in row.items():
        # 15
        if key in ['总建筑面积', '建筑面积', '其中计容面积', '计容面积', '其中地上建筑面积', '地上建筑面积',
                   '其中地下室建筑面积', '地下建筑面积', '地下室建筑面积', '建筑层数', '地下', '用地面积', '建筑高度',
                   '基底面积', '不计容面积']:

            if key == '总建筑面积':
                new_row['建筑面积'] = value

            elif key == '其中计容面积':
                new_row['计容面积'] = value

            elif key == '其中地上建筑面积':
                new_row['地上建筑面积'] = value

            elif key in ['其中地下室建筑面积', '地下建筑面积', '地下室建筑面积']:
                new_row['地下建筑面积'] = value

            elif key == '建筑层数':
                new_row['地上层数'] = value

            elif key == '地下':
                new_row['地下层数'] = value

            else:
                new_row[key] = value

    return [value for key, value in new_row.items()]


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
    for idx in range(2):
        t2_process.append(threading.Thread(target=do_parser, args=(detail_url, DB_url_list, lst10)))

    # 线、进程的开启和等待结束
    for thread in t1_thread:
        thread.start()
    for thread in t2_process:
        thread.start()
    for thread in t1_thread:
        thread.join()
    for thread in t2_process:
        thread.join()

    print("新增数量：", len(lst10))

    # 数据分组，500 一组
    m10 = []
    for index in range(0, len(lst10), 500):
        m10.append(lst10[index:index + 500])

    # 启动线程池
    with ThreadPoolExecutor() as pool:
        pool.map(partial(insert_table_ft, insert_sql_10), m10)

    end = time.time()
    print("耗时: ", end - start)


if __name__ == '__main__':
    main()
