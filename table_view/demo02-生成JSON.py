import random

import DBUtil
import json

"""
    生成GoJs的JSON数据
"""


# def create_json_by_tableName(direction_key,table_name):
#     if direction_key == 'L':
#         where = f"where target_table = '{table_name}'"
#     elif direction_key == 'R':
#         where = f"where source_table = '{table_name}'"
#     else:
#         where = f"where target_table = '{table_name}' or source_table = '{table_name}'"
#
#     sql_code = """
#     select * from u_hq.sql_parse {}
#     """.format(where)
#
#     print(sql_code)
#     pg_util = DBUtil.PgUtil()
#     colors = ['B45200', 'F6F7ED', 'C3CCF2', '3046C8', 'D0FFCA']
#     target_node_key = 0
#     json_obj = {
#         "class": "GraphLinksModel",
#         "linkFromPortIdProperty": "fromPort",
#         "linkToPortIdProperty": "toPort",
#         "modelData": {"position": "-986 -754"},
#         "nodeDataArray": [],
#         "linkDataArray": []
#     }
#     data = pg_util.get_data(sql_code)
#     print(data)
#     json_obj['nodeDataArray'].append({
#         "text": f"{table_name}",
#         "fill": f"tomato",
#         "tipInfo": f"",
#         "category": "Ellipse",
#         "isGroup": False,
#         "key": f"{target_node_key}"
#     })
#     # 添加组
#     group_list = []
#     for row in data:
#         if row[1] == table_name:
#             sourceOrTarget = '--target'
#         else:
#             sourceOrTarget = '--source'
#         jd_key = f"{row[8]}.{row[7]}{sourceOrTarget}"
#         if len(group_list) == 0:
#             if row[1] == table_name:
#                 group_list.append({
#                     "function_name": f"{row[7]}--target",
#                     "jd_list": [jd_key]
#                 })
#             else:
#                 group_list.append({
#                     "function_name": f"{row[7]}--source",
#                     "jd_list": [jd_key]
#                 })
#         else:
#             is_add = False
#             for item in group_list:
#                 if row[7]+sourceOrTarget == item['function_name']:
#                     item['jd_list'].append(jd_key)
#                     item['jd_list'] = list(set(item['jd_list']))[:]
#                     is_add = True
#             if is_add is False:
#                 group_list.append({
#                     "function_name": row[7]+sourceOrTarget,
#                     "jd_list": [jd_key]
#                 })
#     print(json.dumps(group_list))
#     for group_obj in group_list:
#         json_obj['nodeDataArray'].append({
#             "text": f"{group_obj['function_name']}",
#             "fill": f"",
#             "isGroup": True,
#             "key": f"{group_obj['function_name']}"
#         })
#         for jd in group_obj['jd_list']:
#             text = jd.split(".")[0]
#             json_obj['nodeDataArray'].append({
#                 "text": f"{text}",
#                 "fill": "white",
#                 "isGroup": True,
#                 "key": f"{jd}",
#                 "group": f"{group_obj['function_name']}"
#             })
#
#     # 添加节点
#     for row in data:
#         if row[1] == table_name:
#             sourceOrTarget = '--target'
#             color = colors[1]
#         else:
#             sourceOrTarget = '--source'
#             color = colors[2]
#         jd_key = f"{row[8]}.{row[7]}{sourceOrTarget}"
#         json_obj['nodeDataArray'].append({
#             "text": f"{row[1]}",
#             "fill": f"#{color}",
#             "isGroup": False,
#             "key": f"{row[0]}",
#             "group": f"{jd_key}"
#         })
#
#         if row[1] == table_name:
#             json_obj['linkDataArray'].append({
#                 "from": f"{target_node_key}",
#                 "to": f"{row[7]}--target",
#                 "tipInfo": ""
#             })
#         else:
#             json_obj['linkDataArray'].append({
#                 "from": f"{row[7]}--source",
#                 "to": f"{target_node_key}",
#                 "tipInfo": ""
#             })
#     return json.dumps(json_obj)


if __name__ == '__main__':
    result = create_json_by_tableName('R','dw.dwd_spgl_xmdwryhz_bf')
    print(result)
