import ddddocr
import time
import pandas as pd
import requests
from tqdm import tqdm

# 初始化
mass_url = 'https://gd.tzxm.gov.cn/tzxmspweb/api/ProProgress/getProjectInfo?projectCode={}&projectName=&projectYZM={}'
yzm_url = 'https://gd.tzxm.gov.cn/tzxmspweb/captcha?type=hutool&timer{}'
input_file = 'C:\\Users\\元亨利贞\\Desktop\\项目名称待更正情况.xlsx'
output_file = 'C:\\Users\\元亨利贞\\Desktop\\项目名称待更正情况-result.xlsx'
header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}

# 读取工作簿和工作簿中的工作表
data_frame = pd.read_excel(input_file, sheet_name='Result 1')
ocr = ddddocr.DdddOcr()

# 获取项目代码列，转成list
xmdm_list = data_frame['项目代码'].tolist()

xm_list = []
yzm_url

for xmdm in tqdm(xmdm_list):
    for o in range(20):
        # 获取验证码url
        yzm_url = 'https://gd.tzxm.gov.cn/tzxmspweb/captcha?type=hutool&timer{}'
        # 下载验证码
        pic = requests.get(url=yzm_url.format(time.time()), headers=header)
        # 获取图片保存的路径
        path = './img'
        base_url = path + "\{}.jfif".format(xmdm)
        with open(base_url, 'wb') as f:
           f.write(pic.content)
        # 识别验证码
        with open(base_url, 'rb') as f:
           img_bytes = f.read()
        res = ocr.classification(img_bytes)

        mss = requests.get(url=mass_url.format(xmdm, res), headers=header, cookies=pic.cookies)
        json = mss.json()
        try:
            data_list = json.get('data').get('list')
            data = []
            for i in data_list:
                data = [i.get('applyTime'), i.get('auditTypeName'), i.get('projectCode'), i.get('projectName')]
            xm_list.append(data)
            break
        except Exception as r:
           pass

df = pd.DataFrame(xm_list,columns=['申报时间', '类型', '项目代码', '项目名称'])
df.to_excel(output_file, sheet_name='Result', index=False)