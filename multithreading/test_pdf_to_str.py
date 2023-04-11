import re

import PyPDF2


# 打开PDF文件
pdf_file = open('E:\\image3-30\\地字第440606202300001号.pdf', 'rb')

# 创建PDF阅读器对象
pdf_reader = PyPDF2.PdfFileReader(pdf_file)

# 获取PDF文件中的页数
num_pages = pdf_reader.numPages

# 遍历每一页，提取文本内容
for i in range(num_pages):
    page = pdf_reader.getPage(i)
    text = page.extractText()
    print(text)
    print('='*20)
    text = text.replace(" ",'')
    text = text.replace("注:此件由佛山市自然资源局提供，仅供办理政务服务事项时使用，有效期至长期有效","").replace("佛山市自然资源局","")
    text = re.sub(r"\n1、(?:[^\n]+\n)*",'',text)
    print(text)
    obj = []
    for index,link in enumerate(text.splitlines()):
        # 如果这一句的长度小于12，则属于上一行
        if len(link) <= 12:
            if len(obj) == 0:
                obj.append(link)
            # 匹配日期
            elif re.match(r"\d+年\d+月\d+日",link):
                obj.append(link)
            else:
                obj[len(obj)-1] = obj[len(obj)-1]+""+link
        # 匹配施工许可证
        elif len(re.findall(r"。(\d{15})",link))>0:
            new_link= re.sub(r"\d{15}","",link)
            obj.append(new_link)
            obj.append(re.findall(r"。(\d{15})",link)[0])
        else:
            obj.append(link)
    print(obj)


# 关闭文件
pdf_file.close()
