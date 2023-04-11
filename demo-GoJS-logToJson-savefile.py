from sqlparse.tokens import DDL, DML

import DBUtil

import pandas as pd

import json

import sqlparse
from tqdm import tqdm

sql_code = """
select *,row_number() over (partition by function_name order by id) from(
select t1.*,
       row_number() over(partition by t1.function_name,work_id order by t1.begin_time desc nulls last ) as rn
from public.t_exec_log t1
where sql_status = '成功'
) as c
where rn = 1
limit 50;

"""

# json对象 --最后的生成结果
json_obj = {
    "class": "GraphLinksModel",
    "linkFromPortIdProperty": "fromPort",
    "linkToPortIdProperty": "toPort",
    "modelData": {"position": "-986 -754"},
    "nodeDataArray": [],
    "linkDataArray": []
}

# 节点对象 --只选取重要的属性
node_obj = {
    "text": "执行sql脚本",
    "fill": "lightgreen",
    "tipInfo": "",
    "isGroup": True,
    "key": -6,
    "group": -8
}

# 线条对象 --只选取重要属性
line_obj = {
    "from": -1,
    "to": -20
}

if __name__ == '__main__':
    pg_util = DBUtil.PgUtil()

    # 执行SQL，获取数据
    data = pg_util.get_data(sql_code)
    with open("./log.txt", 'wb') as f:
        f.write(str(data).encode())

    # 按照function_name分组
    columns = [
        'id',
        'function_name',
        'work_id',
        'work_step',
        'work_table',
        'tab_counts',
        'begin_time',
        'end_time',
        'fun_time',
        'sql_code',
        'sql_status',
        'sql_log',
        'remark',
        'last_action',
        'row_number'
    ]
    data_df = pd.DataFrame(data, columns=columns)  # .drop(columns=['begin_time', 'end_time'])

    function_name = []
    function_name_list = []
    key_numb = 1000
    line_list = []

    for key, value in data_df.groupby(['function_name', 'work_step']):

        if len(function_name) > 0:
            if key[0] != function_name[len(function_name) - 1]['function_name']:
                function_name_list.append(function_name)
                function_name = []
        if key[1][0].isdigit():
            work_id = ''
        else:
            work_id = value.values.tolist()[0][2]
        function_name.append({"function_name": key[0], "work_id": work_id, "jd_name": key[1], 'key': -key_numb,
                              "data": value.values.tolist()})
        line_list.append(-key_numb)
        key_numb -= 1

    # 以jd_name 作为group节点就是阶段，data作为阶段内 node
    # 给这些node编号，jd_name 也是node, 字段row_number就是key，但是要取负数的
    for function_name in tqdm(function_name_list):
        json_obj['nodeDataArray'].append({"text": "Start", "category": "Ellipse", "fill": "green", "key": 0, })
        last_jd_key = 0
        file_name = ''
        for jd in function_name:
            file_name = jd['function_name']
            if len(json_obj['linkDataArray']) == 0:
                json_obj['linkDataArray'].append({"from": 0, "to": jd['key'], "tipInfo": ""})
            text_front = '' if jd['jd_name'][0].isdigit() else jd['work_id']
            json_obj['nodeDataArray'].append({
                "text": '{} {}'.format(text_front, jd['jd_name']),
                "color": "blue",
                "tipInfo": "",
                "isGroup": True,
                "key": jd['key']
            })
            bf_key = None
            for node in jd['data']:
                if node[10] == '成功':
                    if node[9] is not None:
                        json_obj['nodeDataArray'].append(
                            {"text": "0. 执行时间: {}s".format(node[8]),
                             "category": "RoundedRectangle",
                             "tipInfo": "",
                             "key": -key_numb,
                             "group": jd['key']
                             }
                        )
                        key_numb -= 1
                        # 拆解 SQL 把SQL使用sqlparse
                        sql_list = sqlparse.split(node[9])

                        for index, sql in enumerate(sql_list):
                            index = index + 1
                            # 解析SQL
                            sql_ = sqlparse.format(sql, strip_comments=True)
                            try:
                                stmt = sqlparse.parse(sql_)[0].tokens
                            except:
                                break
                            DDL_key = ''
                            table_name = node[4]

                            for token in stmt:
                                if token.ttype is DDL or token.ttype is DML:
                                    DDL_key = token.value
                                if type(token).__name__ == 'Identifier':
                                    table_name = token.value.split("(")[0].strip()
                                    if '.' in table_name:
                                        model = table_name.split(".")[0]
                                        table = table_name.split(".")[1]
                                        table = table.split(" ")[0]
                                    else:
                                        model = ""
                                        table = "not fond"
                                    break
                            if DDL_key == '':
                                title = str(index) + '. ' + sql_[0:35] + ' ...'
                            else:
                                title = '{}. {} {}.\n{}'.format(index, DDL_key, model, table)
                            # 将此记录 插入 group jd
                            json_obj['nodeDataArray'].append(
                                {
                                    "text": title,
                                    "fill": "lightgreen",
                                    "tipInfo": sql,
                                    "isGroup": False,
                                    "key": -node[14],
                                    "group": jd['key']
                                }
                            )
            json_obj['linkDataArray'].append({"from": jd['key'], "to": jd['key'] + 1, "tipInfo": ""})
            last_jd_key = jd['key']
        json_obj['nodeDataArray'].append({"text": "End", "category": "Ellipse", "fill": "tomato", "key": -10000})
        json_obj['linkDataArray'].append({"from": last_jd_key, "to": -10000, "tipInfo": ""})

        # 保存文件
        path = './gojs_json'
        base_url = path + "\{}.json".format(file_name)
        with open(base_url, 'wb') as f:
            f.write(json.dumps(json_obj).encode())

        # 将json_obj的node和link集合清空
        json_obj['nodeDataArray'] = []
        json_obj['linkDataArray'] = []

    """
    数据结构为：
        [
            [
                {
                    'function_name':'dm_business_sys.p_quality_all_table',
                    'key': -1
                    'jd_name':'10、获取数据生成 dm_business_sys.jcjg_bhgtz_jc',
                    'data':[]
                },
                {
                    'function_name':'dm_business_sys.p_quality_all_table',
                    'jd_name':'10、获取数据生成 dm_business_sys.jcjg_bhgtz_jc',
                    'data':[]
                },
            ]
            
            
            [
                {
                    'function_name':'dm_business_sys.p_quality_all_table',
                    'jd_name':'10、获取数据生成 dm_business_sys.jcjg_bhgtz_jc',
                    'data':[]
                },
                {
                    'function_name':'dm_business_sys.p_quality_all_table',
                    'jd_name':'10、获取数据生成 dm_business_sys.jcjg_bhgtz_jc',
                    'data':[]
                },
            ]
        ]
    """
