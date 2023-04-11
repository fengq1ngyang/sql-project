import os
import re
import PyPDF2
import pandas as pd

import DBUtil

number = 0

result_arr = []


def read_file(filepath,filename):
    global number
    global result_arr
    # 打开PDF文件
    pdf_file = open(filepath, 'rb')

    # 创建PDF阅读器对象
    pdf_reader = PyPDF2.PdfFileReader(pdf_file)

    # 获取PDF文件中的页数
    num_pages = pdf_reader.numPages

    # 遍历每一页，提取文本内容
    result = ''
    for i in range(num_pages):
        page = pdf_reader.getPage(i)
        text = page.extractText()
        # print(text)
        text = text.replace(" ", '')
        text = text.replace("注:此件由佛山市自然资源局提供，仅供办理政务服务事项时使用，有效期至长期有效", "").replace(
            "佛山市自然资源局", "")

        text = re.sub(r"\n1、(?:[^\n]+\n)*", '', text)
        result = result + text

    if len(result) > 20:
        result_arr.append((filename,result))


        # text = text.replace('\n', ' ')
        #
        # obj = []
        # new_obj = {'文件名称':filename,'日期':None,'施工许可证':None,'项目名称':None,'其他':None}
        #
        # # 日期：
        # re_date = re.findall(r"\d{4}\s?年\s?\d{1,2}\s?月\s?\d{1,2}\s?日", text)
        # if len(re_date) > 0:
        #     new_obj['日期'] = re_date[0].replace(" ", '')
        #     text = re.sub(re_date[0], '', text)
        #
        # re_sgxkz = re.findall(r"(\d{15})", text)
        # if len(re_sgxkz) >0:
        #     new_obj['施工许可证'] = re_sgxkz[0]
        #     text = re.sub(re_sgxkz[0], '', text)
        #


        # # print(new_obj)
        #
        # for index, link in enumerate(text.split(' ')):
        #     # 如果这一句的长度小于12，则属于上一行
        #     if len(link) <= 7:
        #         if len(obj) == 0:
        #             obj.append(link)
        #         else:
        #             obj[len(obj) - 1] = obj[len(obj) - 1] + "" + link
        #     else:
        #         # 是否为注开头
        #         if link[0] != '注':
        #             obj.append(link)
        # if len(obj) > 2:
        #     print('=' * 20)
        #     new_obj['项目名称'] = obj[0]
        #     string = ''.join(obj[1:])
        #     try:
        #         new_obj['其他'] = ''.join(filter(lambda x: x.isprintable(), string))
        #     except IllegalCharacterError:
        #         print("String contains illegal characters and cannot be written to Excel")
        #     excel_list.append(new_obj)

    # 关闭文件
    pdf_file.close()

if __name__ == '__main__':
    path = 'E:\\image3-30'

    for filename in os.listdir(path):
        file_path = f"{path}\\{filename}"
        try:
            read_file(file_path,filename)
        except Exception as e:
            print(e)
            print(file_path)

    pg = DBUtil.PgUtil()
    insert_sql = "insert into u_hq.pdf_info (filename,info) values (%s,%s)"
    pg.insert_table(insert_sql,result_arr)
