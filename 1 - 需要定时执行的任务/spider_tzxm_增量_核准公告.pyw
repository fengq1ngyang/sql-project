import json
import re
import time
import requests
import DBUtil
from tqdm import tqdm


COOKIES = {
    'JSESSIONID': '3B715E487B61070B15A654004CB8C8DC',
    '__jsluid_s': '2b7f97d559b4fe6316508de6939bfb9d',
}
HEADER = {
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json;charset=UTF-8',
    'Origin': 'https://gd.tzxm.gov.cn',
    'Referer': 'https://gd.tzxm.gov.cn/PublicityInformation/PublicityHandlingResultsList.html',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.62',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Microsoft Edge";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

DATA_ARRAY = []
ALREADY_EXISTS = []
NOT_EXISTS_PAGE = 0

def download_page(pageNumber):
    global COOKIES
    global HEADER
    global ALREADY_EXISTS

    data = {
        "flag": "10",
        "nameOrCode": "",
        "pageSize": 15,
        "city": "",
        "pageNumber": pageNumber
    }
    data = json.dumps(data)

    base_url = 'https://gd.tzxm.gov.cn/tzxmspweb/api/publicityInformation/selectHzByPage'

    response = requests.post(url=base_url, headers=HEADER, cookies=COOKIES, data=data)
    if response.status_code < 400:
        # 获取返回的cookie
        if 'Set-Cookie' in response.headers:
            j_string = response.headers['Set-Cookie'].split(';')[0]
            COOKIES['JSESSIONID'] = ''.join(re.findall(r"JSESSIONID=(\S+)", j_string))

        text = response.json()
        list = text['data']['list']
        for item in list:
            if item['id'] in ALREADY_EXISTS:
                pass
            else:
                to_baId(item['id'])
    else:
        time.sleep(5)
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!请求出错 url:", response.url)
        print(response.json())


# 获取详情
def to_baId(id):
    global COOKIES
    global DATA_ARRAY
    global HEADER


    data = {
        'id': id
    }
    data = json.dumps(data)
    response = requests.post('https://gd.tzxm.gov.cn/tzxmspweb/api/publicityInformation/getHzggInfoById',headers=HEADER, cookies=COOKIES, data=data)
    text = response.json()
    obj = text['data']
    DATA_ARRAY.append([value for key, value in obj.items()])
    if len(DATA_ARRAY) >= 100:
        insert_DB()


# 插入数据库
def insert_DB():
    global DATA_ARRAY

    insert_sql = """
        INSERT INTO ods.tzxm_project_hzgg (
            proofCode, addTime, handleDeptName, updateTime, pId, sort, title, content, openType, expiryDate, hasStart, copies, stateName, fileNo, finishDateString, finishDate, id, projectName, isValidity
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    pg = DBUtil.PgUtil()
    pg.insert_table(insert_sql, DATA_ARRAY)
    # 插入成功将 DATA_ARRAY 清空
    DATA_ARRAY[:] = []


# 获取数据库中已存在的数据
def get_DB_exists():
    global ALREADY_EXISTS

    select_sql = """
        select pid from ods.tzxm_project_hzgg
    """
    pg = DBUtil.PgUtil()
    data = pg.get_data(select_sql)
    ALREADY_EXISTS = list(map(lambda x: x[0], data))


def get_page():
    global NOT_EXISTS_PAGE
    global ALREADY_EXISTS

    data = {
        "flag": "10",
        "nameOrCode": "",
        "pageSize": 15,
        "city": "",
        "pageNumber": 1
    }
    data = json.dumps(data)
    base_url = 'https://gd.tzxm.gov.cn/tzxmspweb/api/publicityInformation/selectHzByPage'
    response = requests.post(url=base_url, headers=HEADER, cookies=COOKIES, data=data)
    NOT_EXISTS_PAGE = int(response.json()['data']['totalPage']) + 1


if __name__ == '__main__':
    # 1.获取数据库数据
    get_DB_exists()
    # 2.获取需要爬取的页数
    get_page()
    # 3.执行for循环获取数据
    for page in range(1,500):
        download_page(page)
    insert_DB()