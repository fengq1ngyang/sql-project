import pandas as pd
import requests
import time
import re

ak = 'tr93DeWuXfTqYzGfiiFZ8EcdZ9DUmYnW'
api = 'https://api.map.baidu.com/api_recog_address/v1/recog?address={}&ak={}'
input_file = 'C:\\Users\\元亨利贞\\Desktop\\已匹配的项目街镇2.28 - 副本.xls'
output_file = 'C:\\Users\\元亨利贞\\Desktop\\已匹配的项目街镇2.28-result - 副本.xls'


# 读取工作簿和工作簿中的工作表
data_frame = pd.read_excel(input_file, sheet_name='Result')

# 获取地址列，转成list
dz_list = data_frame['地址'].tolist()

# 调用百度地图API
zj_list = []

for index, row in data_frame.iterrows():

    dzs = row['地址']

    # 如果地址中有 、 就安装顿号切分，对切分后的字符串做匹配，若匹配不到“佛山”则在前面加上佛山市
    zjs = []
    for dz in dzs.split("、"):
        if len(dz) > 2:
            pattern = r"佛山"
            matchObj = re.search(pattern, dz)
            if matchObj is None:
                dz = "佛山市" + dz
            time.sleep(0.7)
            r = requests.get(url=api.format(dz, ak))
            json = r.json()
            try:
                q = json.get('admin_info').get('county')
                zj = json.get('admin_info').get('town')
                if(len(q + zj)) > 2:
                    zjs.append(q + zj)
            except Exception as r:
                print(r)
                pass

    row['街镇'] = '、'.join(zjs)
    print(dzs + "   " + str('、'.join(zjs)) + "   " + str(index))
    zj_list.append(row)

df = pd.DataFrame(zj_list)
df.to_excel(output_file, sheet_name='Result', index=False)
