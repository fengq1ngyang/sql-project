
# encoding:utf-8

import requests
import base64
'''
通用文字识别（标准版）
'''

request_url = "https://aip.baidubce.com/rest/2.0/ocr/v1/accurate_basic"
# 二进制方式打开图片文件
f = open('page_0.jpg', 'rb')
img = base64.b64encode(f.read())

params = {"image":img}
access_token = '24.ad54775cad34bbcaed73af149b76dc26.2592000.1683774552.282335-32249598'
request_url = request_url + "?access_token=" + access_token
headers = {'content-type': 'application/x-www-form-urlencoded'}
response = requests.post(request_url, data=params, headers=headers)
if response:
    print (response.json())