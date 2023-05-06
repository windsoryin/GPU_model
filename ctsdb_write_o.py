import time
import json
from elasticsearch6 import Elasticsearch

"""
hosts：
外网地址: 42.193.247.49:9200   内网地址: 172.16.20.3:9200

http_auth：
账号：gnss
密码：YKY7csX#

scheme：http
"""
es = Elasticsearch(hosts=["42.193.247.49:9200"], http_auth=('gnss', 'YKY7csX#'),
                   scheme="http")

action_body = ''
for i in range(2):
    param_index = {"index": {}}
    param_data = {"time": "2023-04-12 06:35:00", "predict_rainfall": 2.512}
    action_body += json.dumps(param_index) + '\n'
    action_body += json.dumps(param_data) + '\n'
print(action_body)

"""
index：predict_rainfall 预测降雨量

doc_type：固定为_doc
"""
result = es.bulk(body=action_body, index="predict_rainfall", doc_type="_doc")
"""
上面返回中的 errors 为 false，代表所有数据写入成功。
items 数组标识每一条记录写入结果，与 bulk 请求中的每一条写入顺序对应。items 的单条记录中，
status 为 2XX 代表此条记录写入成功，_index 标识了写入的 metric 子表，_shards 记录副本写入情况
"""
print(result)
