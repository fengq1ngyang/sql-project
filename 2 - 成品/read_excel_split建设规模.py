import re
import pandas as pd
from concurrent.futures import ProcessPoolExecutor,as_completed
from tqdm import tqdm
import cn2an

def re_str(row):
    # 删除不需要的列
    row = row.drop(['单体名称','附图及附件名称','项目名称','建设单位(个人)','核发日期','建设位置'])
    str = row['建设规模']
    str = str.replace("：", ":").replace("，", ",").replace(" ", "").replace("；", ";").replace(";", ',').replace('（',',').replace('）', ',').split('。')[0].split(",")

    pattern =  r'(\S+):([\S+\.㎡层平方米]+)'

    # Find all matches in the string
    return_list = []
    for s in str:
        matches = re.findall(pattern, s)
        for key,value in matches:
            new_row = row.copy()
            new_row['类型'] = key
            # 如果是长度超过6 而且存在顿号就默认为 value是这种类型 建筑层数：地上1层、30层、32层、地下2层
            if (len(value) > 6 and "、" in value) or "、" in value:
                new_row['值'] = value
                new_row['单位'] = None
                return_list.append(new_row)
            elif re.match(r"地(\S{,4})层",value):
                try:
                    match = re.match(r"地(\S{,4})层", value)
                    new_row['值'] = cn2an.cn2an(match.group(1)[1:], "smart")
                    new_row['单位'] = '层'
                    return_list.append(new_row)
                except:
                    print(str)
            else:
                if "." in value:
                    match = re.search(r"(\d+\.\d+)(\D+)", value)
                else:
                    match = re.search(r"(\d+)(\D+)", value)
                if match:
                    new_row['值'] = match.group(1)
                    new_row['单位'] = match.group(2)
                    try:
                        num = int(new_row['值'])
                    except:
                        num = float(new_row['值'])
                    if num <= 0:
                        pass
                    else:
                        return_list.append(new_row)
                else:
                    if value[0].isdigit():
                        new_row['值'] = value
                        try:
                            num = int(new_row['值'])
                        except:
                            num = float(new_row['值'])
                        if num <= 0:
                            pass
                        else:
                            return_list.append(new_row)
    # row = row.rename(index={'建筑面积':'总建筑面积','计容面积':'其中计容面积','不计容建筑面积':'不计容面积','计容建筑面积':'其中计容面积','建筑基底总面积':'基底面积','地下建筑面积':'其中地下室建筑面积'})
    return return_list


if __name__ == '__main__':
    # 读取Excel
    input_file = 'C:\\Users\\元亨利贞\\Desktop\\zrzy_sert_gcgh.xlsx'
    output_file = 'C:\\Users\\元亨利贞\\Desktop\\zrzy_sert_gcgh-大写数字转阿拉伯.xlsx'

    data = pd.read_excel(input_file,nrows=10000)

    result_list = []

    with ProcessPoolExecutor() as pool:
        futures = [pool.submit(re_str, row) for index, row in data.iterrows() if len(str(row['建设规模'])) > 10]
        for future in tqdm(as_completed(futures), total=len(futures)):
            result_list += future.result()

    df = pd.DataFrame(result_list)
    df.to_excel(output_file)

    print("程序结束")