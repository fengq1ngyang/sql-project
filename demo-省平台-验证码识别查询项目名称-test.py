# 简单的验证码识别
import ddddocr
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
import requests



start_url = 'https://gd.tzxm.gov.cn/'
input_file = 'C:\\Users\\元亨利贞\\Desktop\\项目名称待更正情况.xlsx'
output_file = 'C:\\Users\\元亨利贞\\Desktop\\项目名称待更正情况-result.xlsx'
header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}


# 读取工作簿和工作簿中的工作表
data_frame = pd.read_excel(input_file, sheet_name='Result 1')
# 获取项目代码列，转成list
xmdm_list = data_frame['项目代码'].tolist()


# 先进入首页，点击项目进展查询，开启循环，循环输入项目代码，识别二维码，输入二维码，点击进展查询，获取数据，清空项目代码
# 实例化浏览器
driver = webdriver.Edge()



ocr = ddddocr.DdddOcr()

# 打开网址
driver.get(start_url)
# 最大化浏览器
driver.maximize_window()
driver.find_element(By.CSS_SELECTOR,'#login_api > div.main.div_sty > div:nth-child(2) > label > button:nth-child(6)').click()
xm_list = []

for xmdm in xmdm_list:
    #输入项目代码
    driver.find_element(By.CSS_SELECTOR,'#login_api > div.zzc.zzjzcx.dn > div > p > label:nth-child(2) > input').send_keys(xmdm)
    while True:

        # 获取验证码url
        yzm_url = 'https://gd.tzxm.gov.cn/tzxmspweb/captcha?type=hutool&timer{}'
        # 下载验证码
        pic = requests.get(yzm_url.format(time.time()), headers=header)
        # 获取图片保存的路径
        path = './img'
        base_url = path + "\{}.jfif".format(xmdm)
        with open(base_url, 'wb') as f:
            f.write(pic.content)
        # 识别验证码
        with open(base_url, 'rb') as f:
            img_bytes = f.read()
        res = ocr.classification(img_bytes)
        print(res)
        # 输入验证码
        driver.find_element(By.CSS_SELECTOR,
                            '#login_api > div.zzc.zzjzcx.dn > div > p > label:nth-child(3) > input').send_keys(res)

        # 点击查询
        driver.find_element(By.CSS_SELECTOR, '#login_api > div.zzc.zzjzcx.dn > div > p > button').click()

        # 获取项目名称
        try:
            xmmc = driver.find_element(By.CSS_SELECTOR,
                                       '#login_api > div.zzc.zzjzcx.dn > div > table > tbody > tr:nth-child(3) > td:nth-child(3)').text
        except Exception as e:
            xmmc = 'NoSuch'

        if xmmc != 'NoSuch':
            xm_list.append([xmdm, xmmc])
            break
        # 清空验证码
        time.sleep(2)
        driver.find_element(By.CSS_SELECTOR,
                            '#login_api > div.zzc.zzjzcx.dn > div > p > label:nth-child(3) > input').clear()
        time.sleep(2)

    # 查询成功清空验证码，清空项目代码
    driver.find_element(By.CSS_SELECTOR,'#login_api > div.zzc.zzjzcx.dn > div > p > label:nth-child(2) > input').clear()
    driver.find_element(By.CSS_SELECTOR,'#login_api > div.zzc.zzjzcx.dn > div > p > label:nth-child(3) > input').clear()


df = pd.DataFrame(xm_list)
df.to_excel(output_file, sheet_name='Result', index=False)

