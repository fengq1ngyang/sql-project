from concurrent.futures import ThreadPoolExecutor,as_completed
from tqdm import tqdm
import DBUtil
import re
import cn2an


# 从pgsql查询数据
def get_data_fromPG():
    select_sql = """
        select * 
        from ods.zrzy_sert_gcgh 
        where 
            --"证书编号" like '%2023%' and
            sert_id not in (
                select sert_id from u_hq.zrzy_sert_gcgh_bf_bf
            ) 
    """
    pg = DBUtil.PgUtil()
    rows = pg.get_data(select_sql)
    print("新增数据：",len(rows))
    return rows


# 插入数据到pg
def insert_pg(datas):
    insert_sql = """
        INSERT INTO u_hq.zrzy_sert_gcgh_bf_bf (sert_id, 收件编号, 证书编号, 项目名称, 单体名称, "建设单位(个人)", 核发日期, 建设位置, 建设规模, 附图及附件名称, 建筑面积, 用地面积, 计容面积, 不计容面积, 基底面积, 地上建筑面积, 地下建筑面积, 建筑高度, 地下层数, 地上层数, 管线长, 管径)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    pg = DBUtil.PgUtil()
    pg.insert_table(insert_sql,datas)


def split_jsgm(one_data):
    # 取出 建设规模 字段
    str = one_data[8]

    row = {}

    str = str.replace("：", ":").replace("，", ",").replace(" ", "").replace("；", ";").replace(";", ',').replace('（',',').replace('）', ',').split('。')[0].split(",")

    pattern = r'(\S+):([\S+\.㎡层平方米]+)'

    # 新的字典确保顺序，以及完整性
    new_row = {'建筑面积': None, '用地面积': None, '计容面积': None, '不计容面积': None, '基底面积': None,
               '地上建筑面积': None,
               '地下建筑面积': None, '建筑高度': None, '地下层数': None, '地上层数': None,
               '管线长': None, '管径': None}

    # 判断是否为纯数字


    # Find all matches in the string
    for s in str:
        is_num = False
        num = ''
        try:
            num = float(s)
            is_num = True
        except ValueError:
            pass

        # 判断是否为 管线长:xxxx;管径为xxx类型数据
        if '管线' in s :
            re_ = r"(\d+)(\.\d+)?"
            reso_arr = re.findall(re_,s)
            for item in reso_arr:
                new_row['管线长'] = ''.join(list(item))
        elif '管径' in s:
            s = s.replace(":",'')
            re_date = re.findall(r"([\u4e00-\u9fa5])", s)
            for key in re_date:
                s = s.replace(key, '')
            new_row['管径'] = s
            pass
        # 判断是否包含数字
        elif is_num:
            new_row['建筑面积'] = num
        else:
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
                        # print(str)
                        pass
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

    for key, value in row.items():
        # 15
        if key in ['总建筑面积', '建筑面积', '其中计容面积', '计容面积', '其中地上建筑面积', '地上建筑面积',
                   '其中地下室建筑面积', '地下建筑面积', '地下室建筑面积', '建筑层数', '地下', '用地面积', '建筑高度',
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

            elif key == '地下':
                new_row['地下层数'] = value

            else:
                new_row[key] = value

    split_arr = [value for key, value in new_row.items()]
    one_data = [ index for index in one_data[0:10]]
    # 数组拼接
    return one_data + split_arr


if __name__ == '__main__':
    # 获取数据库数据
    rows = get_data_fromPG()

    # 所有数据集
    result = []

    with ThreadPoolExecutor() as pool:
        futures = []
        for row in rows:
            futures.append(pool.submit(split_jsgm,row))
        for future in tqdm(as_completed(futures), total=len(futures)):
            result.append(future.result())

    with ThreadPoolExecutor() as pool:
        for index in range(0,len(result),300):
            pool.submit(insert_pg,result[index:index+300])


    print("执行结束")