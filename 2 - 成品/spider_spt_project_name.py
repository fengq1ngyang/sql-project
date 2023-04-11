import requests
import pandas as pd
import ddddocr
import time
import concurrent.futures


header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'
}
base_url = 'https://gd.tzxm.gov.cn/tzxmspweb/api/ProProgress/getProjectInfo?projectCode={}&projectName=&projectYZM={}'
file_path = 'C:\\Users\\元亨利贞\\Desktop\\2023年签订的总包合同1-result.xlsx'
output_file = 'C:\\Users\\元亨利贞\\Desktop\\2023年签订的总包合同2-result.xlsx'

"""读取Excel文件"""
def read_excel():
    dataframe = pd.read_excel(file_path)
    return dataframe


"""识别验证码"""
def read_yzm(ocr):
    yzm_url = 'https://gd.tzxm.gov.cn/tzxmspweb/captcha?type=hutool&timer{}'
    pic = requests.get(url=yzm_url.format(time.time()), headers=header)
    projectYZM = ocr.classification(pic.content)
    return (projectYZM, pic.cookies)


"""下载网页"""
def do_dowload(url,cookies):
    html = requests.get(url=url,cookies=cookies,headers=header)
    return html

"""解析html"""
def do_parse(html):
    obj = html.json()
    data_list = obj.get('data').get('list')
    # print(data_list)
    if len(data_list) == 0:
        return (None,None)
    return (data_list[0].get('projectCode'),data_list[0].get('projectName'))

"""逻辑main函数，线程池执行"""
def main(row):
    gcdm = row['补充项目代码']
    for i in range(20):
        projectYZM, cookie = read_yzm(ocr)
        url_ = base_url.format(gcdm, projectYZM)
        html = do_dowload(url_, cookie)
        try:
            obj = do_parse(html)
            row['项目名称'] = obj[1]
            row['prj_code'] = obj[0]
            return row
        except:
            pass

if __name__ == '__main__':
    start = time.time()
    ocr = ddddocr.DdddOcr()
    data_list = read_excel()
    reslt_list = []
    with concurrent.futures.ThreadPoolExecutor() as pool:
        objs = []
        for index, row in data_list.iterrows():
            obj = pool.submit(main,row)
            objs.append(obj)

    for obj in objs:
        reslt_list.append(obj.result())

    new_df = pd.DataFrame(reslt_list)
    print(new_df)
    new_df.to_excel(output_file, sheet_name='Result', index=False)
    end = time.time()
    print(f"耗时：{end-start}")