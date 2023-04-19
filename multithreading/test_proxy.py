from fake_useragent import UserAgent
import requests

request_body = {
    "strWhere": '2023',
    "action": 'gcgh',
    "area": '',
    "pageIndex": 1,
    "pageSize": 15,
}
base_url = 'https://zsxx.fszrzy.foshan.gov.cn/GTGHService/home/SearchData'
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


# print(requests.get("http://127.0.0.1:8000/count/").json())

proxy = {
    'https': 'http://127.0.0.1:7890',
    'http': 'http://127.0.0.1:7890'
}



for _ in range(0,100):
    response = requests.post(url=base_url, data=request_body, proxies=proxy,headers=headers, verify=False)
    print(response.request)
    body = response.text
    print(body)

    from urllib.request import getproxies
    # 查看代理配置 {'http': 'http://127.0.0.1:7890', 'https': 'https://127.0.0.1:7890', 'ftp': 'ftp://127.0.0.1:7890'}
    print(getproxies())