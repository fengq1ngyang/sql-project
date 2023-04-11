import  re


# str = '建筑层数：地上1层、30层、32层、地下2层'
# str = str.replace("：", ":").replace("，", ",").replace(" ", "").replace("；", ";").replace(";", ',').replace('（', ',').replace(
#     '）', ',').split('。')[0].split(",")
#
# for s in str:
#     text = re.findall(r'(\S+):([\S+\.㎡层平方米]+)', s)
#
#     print(text)


str = '㎡，基底面 积:1573.78㎡。 2023年1月19日"'

re_date = re.findall(r"\d{4}\s?年\s?\d{1,2}\s?月\s?\d{1,2}\s?日", str)
print(re_date)