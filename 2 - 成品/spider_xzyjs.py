import time
import requests
from bs4 import BeautifulSoup
import json
import DBUtil
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

base_url = 'https://zsxx.fszrzy.foshan.gov.cn/GTGHService/home/SearchData'
output_file = 'C:\\Users\\元亨利贞\\Desktop\\xzyjsAll-result.xlsx'
insert_sql ="""insert into ods.zrzy_sert_xzyjs(收件编号,证书编号,项目名称,建设单位,核发日期,建设项目拟选位置,拟用地面积（平方米）,拟建设规模,建设项目依据,附图及附件名称,sert_id) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

pgutil = DBUtil.PgUtil()

def get_detail_url(page):
    request_body = {
        "strWhere": '',
        "action": 'xzyjs',
        "area": '',
        "pageIndex": page,
        "pageSize": 10000,
    }

    response = requests.post(url=base_url, data=request_body, verify=False)
    body = response.json()
    uuids = json.loads(body['datas'])
    detail_url_list = []
    for uuid in uuids:
        url_ = f"https://zsxx.fszrzy.foshan.gov.cn/GTGHService/ViewCase/jsxmghxzyjs/{uuid['4']}"
        detail_url_list.append(url_)
    print(f"当前page：{page},len: {len(detail_url_list)}")
    return detail_url_list

# #字典类型（存入Excel）
# def parse(detail_url):
#     html = requests.get(url=detail_url, verify=False).text
#     soup = BeautifulSoup(html, 'html.parser')
#     table = soup.find('div', {'id': 'yslz-info'}).find('table')
#     fields = {}
#     for row in table.find_all('tr')[1:]:
#         key = row.find('th').text.strip()  # strip方法去除任何前导或尾随空格
#         value = None
#         if row.find('td') is not None:
#             value = row.find('td').text.strip()
#         fields[key] = value
#     fields['sert_id'] = detail_url.split('/')[-1]
#     print(fields)
#     return fields

# 元组类型（存入pgsql）
def parse(detail_url):
    html = requests.get(url=detail_url, verify=False).text
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('div', {'id': 'yslz-info'}).find('table')
    fields = []
    for row in table.find_all('tr')[1:]:
        value = 'None'
        if row.find('td') is not None:
            value = row.find('td').text.strip()
        fields.append(value)
    fields.append(detail_url.split('/')[-1])
    return fields



if __name__ == '__main__':
    start = time.time()
    pageNum = 1
    result_list = []
    for page in range(1, pageNum + 1):
        # 得到当前页所有url
        nowPage_url_list = get_detail_url(page)
        for url_ in nowPage_url_list:
            # 得到当前url的数据
            obj = parse(url_)
            result_list.append(obj)
    # print(len(result_list),len(set(result_list)))
    # df = pd.DataFrame(result_list)
    # df.drop_duplicates(inplace=True)
    # df.to_excel(output_file, sheet_name='Result', index=False)
    pgutil.insert_table(insert_sql,result_list)


    end = time.time()
    print("耗时：",end-start)