import time
import json
from elasticsearch6 import Elasticsearch
import numpy as np
import time
import datetime
"""
hosts：
外网地址: 42.193.247.49:9200   内网地址: 172.16.20.3:9200

http_auth：
账号：gnss
密码：YKY7csX#

scheme：http
"""
loaddata = np.load(
    './results/informer_ftMS_sl96_ll48_pl24_dm512_nh8_el2_dl1_df2048_atprob_fc5_ebtimeF_dtTrue_mxTrue_test_0/real_prediction_tp.npy')

es = Elasticsearch(hosts=["42.193.247.49:9200"], http_auth=('gnss', 'YKY7csX#'),
                   scheme="http")

action_body = ''
for i in range(len(loaddata)):
    tmp_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tmp_rf = loaddata[0,i,0].astype(float)
    param_index = {"index": {"_type": "doc"}}
    param_data = {"time": tmp_time, "predict_rainfall": tmp_rf}
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
