import  re


# str = '建筑层数：地上1层、30层、32层、地下2层'
# str = str.replace("：", ":").replace("，", ",").replace(" ", "").replace("；", ";").replace(";", ',').replace('（', ',').replace(
#     '）', ',').split('。')[0].split(",")
#
# for s in str:
#     text = re.findall(r'(\S+):([\S+\.㎡层平方米]+)', s)
#
#     print(text)


str = """
道路长度为0米;道路宽度为0米。
管线长度为右线长度788.022米 左线长度789.547米。
管径为外径6m,内径5.4m。
"""

re_date = re.findall(r"(\d+)(\.\d+)?", str)
print(re_date)

str = """
管径为DN100-DN200
"""
re_date = re.findall(r"([\u4e00-\u9fa5])", str)
for key in re_date:
    str = str.replace(key,'')
print(str)

str = "123"
try:
  num = float(str)
  print("是数字")
except ValueError:
  print("不是数字")