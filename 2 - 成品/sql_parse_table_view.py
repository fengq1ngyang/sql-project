#!/usr/bin/python

import psycopg2
import psycopg2.extras
import sqlparse
from flask import Flask, request
from flask_cors import CORS
from sqlparse.tokens import DDL, DML
import json
import datetime
import re

app = Flask(__name__)


@app.route('/tableViewGet', methods=["GET"])
def calculateGet():
    print(request)
    table_name = request.args.get("table_name", 0)

    res = in_out_table_parse(table_name)
    return res


@app.route('/tableViewPost', methods=["POST"])
def calculatePost():
    params = request.form if request.form else request.json
    print(params)
    table_name = params.get("table_name", 0)

    res = in_out_table_parse(table_name)
    return res


""" 定义接口，返回流程图JSON数据 """


@app.route('/getjson', methods=['GET'])
def get_json():
    function_name = request.args.get('functionName', default='not found!')
    return create_json(function_name)


# @app.route('/getTableLorR', methods=['GET'])
# def get_table_LorR():
#     direction_key = request.args.get('direction_key', default='not found!')
#     table_name = request.args.get('table_name', default='not found!')
#     table_key = request.args.get('table_key', default='-1')
#     return create_json_by_tableName(direction_key, table_name, table_key)

@app.route('/getTableLorR', methods=['POST'])
def process_request():
    data = json.loads(request.data)
    direction_key = data['direction_key']
    table_name = data['table_name']
    table_key = data['table_key']
    # 处理请求体数据，根据具体需求编写代码
    response = create_json_by_tableName(direction_key, table_name, table_key)
    return response


# 根据表名和左边右边，生成JSON
def create_json_by_tableName(direction_key, table_name, table_key):
    if direction_key == 'L':
        where = f"where target_table = '{table_name}' and id != {table_key} group by id having source_table != '{table_name}'"
    elif direction_key == 'R':
        where = f"where source_table = '{table_name}' and id != {table_key} group by id having target_table != '{table_name}'"
    else:
        where = f"where target_table = '{table_name}' or source_table = '{table_name}'"

    sql_code = """
    select * from u_hq.sql_parse {} 
    """.format(where)

    pg_util = PgUtil()
    colors = ['B45200', 'F6F7ED', 'C3CCF2', '3046C8', 'D0FFCA']
    json_obj = {
        "class": "GraphLinksModel",
        "linkFromPortIdProperty": "fromPort",
        "linkToPortIdProperty": "toPort",
        "modelData": {"position": "-986 -754"},
        "nodeDataArray": [],
        "linkDataArray": []
    }
    data = pg_util.get_data(sql_code)
    if direction_key != 'R' and direction_key != 'L':
        json_obj['nodeDataArray'].append({
            "text": f"{table_name}",
            "fill": f"tomato",
            "tipInfo": f"0",
            "category": "Ellipse",
            "isGroup": False,
            "key": f"{table_name}"
        })
    # 添加组
    group_list = []
    for row in data:
        if row[1] == table_name:
            sourceOrTarget = '--target'
        else:
            sourceOrTarget = '--source'
        # = function_name + 阶段名称 + source/target + work_id + sql_row_number + 开始时间
        time_ = row[11].strftime("%Y-%m-%d %H:%M:%S")
        sql_row_key = f"{row[5]}?{row[6]}{sourceOrTarget}?{row[9]}-{row[8]}?({time_})"
        if len(group_list) == 0:
            if row[1] == table_name:
                group_list.append({
                    "function_name": f"{row[5]}--target",
                    "jd_list": [sql_row_key]
                })
            else:
                group_list.append({
                    "function_name": f"{row[5]}--source",
                    "jd_list": [sql_row_key]
                })
        else:
            is_add = False
            for item in group_list:
                if row[5] + sourceOrTarget == item['function_name']:
                    item['jd_list'].append(sql_row_key)
                    item['jd_list'] = list(set(item['jd_list']))[:]
                    is_add = True
            if is_add is False:
                group_list.append({
                    "function_name": row[5] + sourceOrTarget,
                    "jd_list": [sql_row_key]
                })
    for group_obj in group_list:
        json_obj['nodeDataArray'].append({
            "text": f"{group_obj['function_name']}",
            "fill": f"",
            "tipInfo": f"0",
            "isGroup": True,
            "key": f"{group_obj['function_name']}"
        })
        for jd in group_obj['jd_list']:
            sortKey = jd.split("?")[2]
            start_time = jd.split("?")[3]
            json_obj['nodeDataArray'].append({
                "text": f"{sortKey}{start_time}",
                "fill": "white",
                "tipInfo": f"{sortKey}",
                "isGroup": True,
                "key": f"{jd}",
                "group": f"{group_obj['function_name']}"
            })

    # 二维数组排序
    json_obj['nodeDataArray'] = sorted(json_obj['nodeDataArray'], key=lambda x: x["tipInfo"])

    # 添加节点
    for row in data:
        if row[1] == table_name:
            sourceOrTarget = '--target'
            color = colors[1]
            time_ = row[11].strftime("%Y-%m-%d %H:%M:%S")
            sql_row_key = f"{row[5]}?{row[6]}{sourceOrTarget}?{row[9]}-{row[8]}?({time_})"
            json_obj['nodeDataArray'].append({
                "text": f"{row[3]}",
                "fill": f"#{color}",
                "isGroup": False,
                "tipInfo": f"--{row[6]}\n{row[7]}",
                "key": f"{row[0]}",
                "group": f"{sql_row_key}"
            })

        else:
            sourceOrTarget = '--source'
            color = colors[2]
            time_ = row[11].strftime("%Y-%m-%d %H:%M:%S")
            sql_row_key = f"{row[5]}?{row[6]}{sourceOrTarget}?{row[9]}-{row[8]}?({time_})"
            json_obj['nodeDataArray'].append({
                "text": f"{row[1]}",
                "fill": f"#{color}",
                "isGroup": False,
                "tipInfo": f"--{row[6]}\n{row[7]}",
                "key": f"{row[0]}",
                "group": f"{sql_row_key}"
            })
        # 解析SQL得到关键字
        sql_ = sqlparse.format(row[7], reindent=True, keyword_case='upper', strip_comments=True)
        try:
            stmt = sqlparse.parse(sql_)[0].tokens
        except:
            pass
        DDL_key = ''
        for token in stmt:
            if token.ttype is DDL or token.ttype is DML:
                DDL_key = token.value
                break
        if row[1] == table_name:
            is_onArray = False
            for link_obj in json_obj['linkDataArray']:
                if link_obj['to'] == sql_row_key:
                    is_onArray = True
            if not is_onArray:
                from_ = table_name
                if table_key != '-1':
                    from_ = table_key
                json_obj['linkDataArray'].append({
                    "from": f"{from_}",
                    "to": f"{sql_row_key}",
                    "tipInfo": f"{DDL_key}",
                    "scale": 2,
                    "margin": 10,
                    "font": "bold 28pt Helvetica,bold Arial,sans-serif"
                })
        else:
            is_onArray = False
            for link_obj in json_obj['linkDataArray']:
                if link_obj['from'] == sql_row_key:
                    is_onArray = True
            if not is_onArray:
                to_ = table_name
                if table_key != '-1':
                    to_ = table_key
                json_obj['linkDataArray'].append({
                    "from": f"{sql_row_key}",
                    "to": f"{to_}",
                    "tipInfo": f"{DDL_key}",
                    "scale": 2,
                    "margin": 10,
                    "font": "bold 28pt Helvetica,bold Arial,sans-serif"
                })
    return json.dumps(json_obj)


# SQL执行函数，返回查询结果列表
def sql_inquiry(sql):
    conn = psycopg2.connect(dbname="fsdrm", user="fsdrm", password="yhlz@4185_20220426", host="172.16.0.127",
                            port="5432")
    conn.set_client_encoding('utf-8')
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql)
    result = cur.fetchall()

    conn.commit()
    cur.close()
    conn.close()
    return result


def in_out_table_parse(table_name):
    json_str = '{"class":"GraphLinksModel","linkFromPortIdProperty":"fromPort","linkToPortIdProperty":"toPort","modelData": {"position":"-100 -100"},"nodeDataArray":[],"linkDataArray":[]}'

    model = json.loads(json_str)

    nodes = []
    links = []

    node_sql = '''
        select 'g'||function_id as function_id,function_name,work_step,detail_table as table_name, step_sql,step_type,
               concat(substr(step_type,1,1),'_in_',detail_table_id) as table_id ,'in' as direction
        from dw_temp.t_exec_sql_parse where step_table ='{0}'
        union
        select 'g'||function_id as function_id,function_name,work_step, step_table as table_name,  step_sql,step_type,
               concat(substr(step_type,1,1),'_out_',step_table_id) as table_id ,'out' as direction
        from dw_temp.t_exec_sql_parse where detail_table ='{0}'
        union
        select  null as function_id, '' as function_name,''as work_step, detail_table as table_name,'' as step_sql,'' as step_type,
               concat('',detail_table_id)as table_id ,'' as direction
        from dw_temp.t_exec_sql_parse where detail_table ='{0}'
    '''.format(table_name)

    for row in sql_inquiry(node_sql):
        node = {}
        node['key'] = row['table_id']
        node['text'] = row['table_name']

        node['tipInfo'] = '--{0}:{1}\n{2}'.format(row['function_name'], row['work_step'], row['step_sql'])

        if row['direction'] == 'in':
            node['category'] = 'Rectangle'
            node['fill'] = 'lightgreen'
        elif row['direction'] == 'out':
            node['category'] = 'Rectangle'
            node['fill'] = 'lightskyblue'
            if row['function_id'] is not None:
                node['group'] = row['function_id']
        else:
            node['category'] = 'Ellipse'
            node['fill'] = 'tomato'
        nodes.append(node)

    group_sql = "select distinct function_id,function_name from (" + node_sql + ") a where direction ='out' "
    for group in sql_inquiry(group_sql):
        if group['function_id'] is not None:
            node = {}
            node['key'] = group['function_id']
            node['text'] = group['function_name']
            node['isGroup'] = 'true';
            node['color'] = 'blue'
            nodes.append(node)

    link_sql = '''
        select detail_table,step_type,step_table,substr(step_type,1,1) as sql_type,
               concat(substr(step_type,1,1),'_in_',detail_table_id) as detail_table_id,
               concat('',step_table_id) as step_table_id
        from dw_temp.t_exec_sql_parse where step_table ='{0}'
        union
        select detail_table,step_type,step_table,substr(step_type,1,1) as sql_type,
               concat('',detail_table_id) as detail_table_id,
               concat(substr(step_type,1,1),'_out_',step_table_id) as step_table_id
        from dw_temp.t_exec_sql_parse where detail_table ='{0}'
    '''.format(table_name)

    for row in sql_inquiry(link_sql):
        link = {}
        link['from'] = row['detail_table_id']
        link['to'] = row['step_table_id']
        link['fill'] = ''
        link['tipInfo'] = row['step_type']
        links.append(link)

    model['nodeDataArray'] = nodes
    model['linkDataArray'] = links

    model_json = json.dumps(model, sort_keys=False, indent=4, separators=(',', ': '))
    return model_json


""" pg数据库工具类 """


class PgUtil:
    def __init__(self):
        # 创建连接对象
        self.conn = None
        self.cur = None
        self.database = "fsdrm"
        self.user = "hehongquan"
        self.password = "yhlz@123"
        self.host = "172.16.0.127"
        self.port = "5432"

    def get_conn(self):
        """ 获取conn """
        self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host,
                                     port=self.port)
        self.cur = self.conn.cursor()  # 创建指针对象

    def close(self):
        """ 关闭连接 """
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def get_data(self, sql):
        """ 查询数据 """
        res = None
        try:
            self.get_conn()
            self.cur.execute(sql)
            res = self.cur.fetchall()
            self.close()
        except Exception as e:
            print(e)
            self.close()
            pass
        return res


""" json 生成"""


def create_json(function_name):
    # json对象 --最后的生成结果
    json_obj = {
        "class": "GraphLinksModel",
        "linkFromPortIdProperty": "fromPort",
        "linkToPortIdProperty": "toPort",
        "modelData": {"position": "-986 -754"},
        "nodeDataArray": [],
        "linkDataArray": []
    }
    sql_code = """
with temp1 as (select function_name,
                      max(begin_time) as last_action
               from public.t_exec_log
               group by function_name)
               
select c.*,row_number() over (partition by c.function_name order by id) from(
select t1.*,
       row_number() over(partition by t1.function_name,work_id order by t1.begin_time desc nulls last ) as rn
from public.t_exec_log t1
where sql_status = '成功' and function_name = '{}'
) as c left join temp1 t2 on c.function_name=t2.function_name
where rn = 1 and date_trunc('day', t2.last_action) = date_trunc('day', c.begin_time);
"""
    pg_util = PgUtil()
    # 执行SQL，获取数据
    data = pg_util.get_data(sql_code.format(function_name))
    # 将list转成dataframe类型，便于数据分组

    # 数据存储变量定义
    function_name = []
    key_numb = 1000

    # 数据包装
    for index, value in enumerate(data):
        time_ = value[6].strftime("%Y-%m-%d %H:%M:%S")
        function_name.append(
            {"function_name": f"{value[1]}",
             "work_id": value[2],
             "jd_name": f"用时{value[8]}s({time_})\n{value[3]}",
             "color": 'red' if value[8] >= 60 else '',
             'key': -key_numb,
             "data": [value]})
        key_numb -= 1

    # 以jd_name 作为group节点就是阶段，data作为阶段内 node
    # 给这些node编号，jd_name 也是node, 字段row_number就是key，但是要取负数的

    json_obj['nodeDataArray'].append({"text": "Start", "category": "Ellipse", "fill": "green", "key": 0, })

    last_jd_key = 0
    for index_func, jd in enumerate(function_name):
        if len(json_obj['linkDataArray']) == 0:
            json_obj['linkDataArray'].append({"from": 0, "to": jd['key'], "tipInfo": ""})
        text_front = jd['work_id']  # '' if jd['jd_name'][0].isdigit() else jd['work_id']
        json_obj['nodeDataArray'].append({
            "text": '{} {}'.format(text_front, jd['jd_name']),
            "color": jd['color'],
            "tipInfo": "",
            "isGroup": True,
            "key": jd['key']
        })
        for node in jd['data']:

            if node[10] == '成功':
                if node[9] is not None:
                    # json_obj['nodeDataArray'].append(
                    #     {"text": "0. 执行时间: {}s".format(node[8]),
                    #      "category": "RoundedRectangle",
                    #      "tipInfo": "",
                    #      "key": -key_numb,
                    #      "group": jd['key']
                    #      }
                    # )
                    key_numb -= 1
                    # 拆解 SQL 把SQL使用sqlparse
                    sql_list = sqlparse.split(node[9])

                    for index, sql in enumerate(sql_list):
                        error_color = 'lightgreen'
                        index = index + 1
                        # 解析SQL
                        sql_ = sqlparse.format(sql, strip_comments=True)
                        try:
                            stmt = sqlparse.parse(sql_)[0].tokens
                        except:
                            break
                        DDL_key = ''
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
                            spl_sql_ = sql_.split('table')
                            try:
                                table_name_split = spl_sql_[1].split('.')
                            except:
                                if 'update' in sql_:
                                    arr = re.split(r"\s+", sql_)
                                    table_name_index = arr.index('update')+1
                                    spl_sql_ = ['update']
                                    table_name_split = arr[table_name_index].split('.')
                                else:
                                    spl_sql_ = [sql_[0:25],'']
                                    table_name_split = ['...','SQL解析失败！']
                                    error_color = '#EBD7D7'
                            title = f"{str(index)}. {spl_sql_[0]} {table_name_split[0]}.\n{table_name_split[1]}"
                        else:
                            title = '{}. {} {}.\n{}'.format(index, DDL_key, model, table)
                        # 将此记录 插入 group jd
                        json_obj['nodeDataArray'].append(
                            {
                                "text": title,
                                "fill": "lightgreen",
                                "tipInfo": sql,
                                "isGroup": False,
                                "fill": error_color,
                                "key": -node[14],
                                "group": jd['key']
                            }
                        )
        if index_func != len(function_name) - 1:
            json_obj['linkDataArray'].append({"from": jd['key'], "to": jd['key'] + 1, "tipInfo": ""})
        last_jd_key = jd['key']
    json_obj['nodeDataArray'].append({"text": "End", "category": "Ellipse", "fill": "tomato", "key": -10000})
    json_obj['linkDataArray'].append({"from": last_jd_key, "to": -10000, "tipInfo": ""})
    # JSON结果就存储与 json_obj 变量中
    model = json.dumps(json_obj)
    return model


CORS(app, resources=r'/*')
if __name__ == '__main__':
    # 1、按表生产JSON文件
    # table_name = 'dw.dwd_ht_contract'
    table_name = 'dw.dwd_spgl_xmdwryhz_bf'

    model_json = in_out_table_parse(table_name)

    file_name = 'D:\\python\\' + table_name + '.json'
    print(file_name)
    fo = open(file_name, "w")
    fo.write(model_json)
    fo.close()
    # 2、启动HTTP服务对外提供服务
    app.run(host='0.0.0.0', threaded=True, debug=False, port=8080)
