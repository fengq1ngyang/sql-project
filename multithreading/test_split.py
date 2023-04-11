import re
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
import cn2an
import requests

"""
# 切分建筑规模
def split_jzgm(str):
    # 解析后的字典对象
    row = {}

    str = str.replace("：", ":").replace("，", ",").replace(" ", "").replace("；", ";").replace(";", ',').replace('（',
                                                                                                               ',').replace(
        '）', ',').split('。')[0].split(",")

    pattern = r'(\S+):([\S+\.㎡层平方米]+)'

    # Find all matches in the string
    for s in str:
        matches = re.findall(pattern, s)
        for key, value in matches:
            # 如果是长度超过6 而且存在顿号就默认为 value是这种类型 建筑层数：地上1层、30层、32层、地下2层
            if (len(value) > 6 and "、" in value) or "、" in value:
                row[key] = value

            elif re.match(r"地(\S{,4})层", value):
                try:
                    match = re.match(r"地(\S{,4})层", value)
                    row[key] = int(cn2an.cn2an(match.group(1)[1:], "smart"))
                except:
                    print(str)
            else:
                if "." in value:
                    match = re.search(r"(\d+\.\d+)(\D+)", value)
                else:
                    match = re.search(r"(\d+)(\D+)", value)
                if match:
                    try:
                        num = int(match.group(1))
                    except:
                        num = float(match.group(1))
                    if num <= 0:
                        pass
                    else:
                        row[key] = match.group(1)
                else:
                    if value[0].isdigit():
                        try:
                            num = int(value)
                        except:
                            num = float(value)
                        if num <= 0:
                            pass
                        else:
                            row[key] = value
    # 新的字典确保顺序，以及完整性
    new_row = {'建筑面积': None, '用地面积': None, '计容面积': None, '不计容面积': None, '基底面积': None, '地上建筑面积': None,
               '地下建筑面积': None, '建筑高度': None, '地下层数': None, '地上层数': None}
    for key, value in row.items():
        # 15
        if key in ['总建筑面积', '建筑面积', '其中计容面积', '计容面积', '其中地上建筑面积', '地上建筑面积',
                   '其中地下室建筑面积', '地下建筑面积', '地下室建筑面积', '建筑层数', '地上', '用地面积', '建筑高度',
                   '基底面积', '不计容面积']:

            if key == '总建筑面积':
                new_row['建筑面积'] = value

            elif key == '其中计容面积':
                new_row['计容面积'] = value

            elif key == '其中地上建筑面积':
                new_row['地上建筑面积'] = value

            elif key in ['其中地下室建筑面积', '地下建筑面积', '地下室建筑面积']:
                new_row['地下建筑面积'] = value

            elif key == '建筑层数':
                new_row['地上层数'] = value

            elif key == '地上':
                new_row['地上层数'] = value

            else:
                new_row[key] = value

    print([value for key, value in new_row.items()])
"""



if __name__ == '__main__':
    str = """
建筑面积：7920平方米，其中计容面积：15840平方米，不计容面积：0平方米，建筑层数：地上壹层
"""
    # str = str.replace("：",":").replace("，",",").replace(" ","").replace("；",";").replace(";",',').replace('（',',').replace('）',',').split('。')[0].split(",")
    # pattern =  r'(\S+):([\d\.㎡层平方米]+)'
    #
    # # Find all matches in the string
    # msg = {}
    # for s in str:
    #     matches = re.findall(pattern, s)
    #     for key,value in matches:
    #         msg[key] = value
    #
    # msg = pd.Series(msg)
    # msg = msg.rename(index={'建筑面积':'总建筑面积'})
    # print(msg)

    # ma = ['建筑层数:地上2层']
    # pattern =  r'(\S+):([\d\.㎡层平方米]+)'
    # row = {}
    # return_list = []
    # for s in str:
    #     matches = re.findall(pattern, s)
    #     for key, value in matches:
    #         new_row = row.copy()
    #         new_row['类型'] = key
    #         if "." in value:
    #             match = re.search(r"(\d+\.\d+)(\D+)", value)
    #         else:
    #             match = re.search(r"(\d+)(\D+)", value)
    #         if match:
    #             new_row['值'] = match.group(1)
    #             new_row['单位'] = match.group(2)
    #             return_list.append(new_row)
    #         else:
    #             if value[0].isdigit():
    #                 new_row['值'] = value
    #                 return_list.append(new_row)
    # print(return_list)
    str = """
        总建筑面积:417.72㎡，其中地下室建筑面积:0㎡，建筑层数:地上4层，地下:0层，用地面积:104.43㎡，计容面积:417.72㎡，基底面积:104.43㎡。
    """

    field = {'收件编号': None, '证书编号': None, '项目名称': None, '单体名称': None, '建设单位(个人)': None,
             '核发日期': None, '建设位置': None, '建设规模': None, '附图及附件名称': None, 'sert_id': None, }
    key = '收件编号1'
    if key in field.keys():
        print(key)