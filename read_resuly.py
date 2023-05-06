import numpy as np
from elasticsearch6 import Elasticsearch
import pandas as pd
import time
import json
site_name='B04'
now_time=time.time()

loaddata = np.load(
        './results/informer_ftMS_sl96_ll48_pl24_dm512_nh8_el2_dl1_df2048_atprob_fc5_ebtimeF_dtTrue_mxTrue_test_0/real_prediction_{}.npy'.format(
            site_name))
data_log = pd.DataFrame(loaddata[:,:,0], index=['prediction_rainfall'])
data_log.insert(data_log.shape[1],'time',pd.to_datetime(now_time, unit='s'))
data_log.to_csv('./log/prediction_log.csv', mode='a')
es = Elasticsearch(hosts=["42.193.247.49:9200"], http_auth=('gnss', 'YKY7csX#'),
                   scheme="http")
action_body = ''
s=loaddata.shape[1]
for i in range(loaddata.shape[1]):
    tmp_time = pd.to_datetime(now_time, unit='s').strftime('%Y-%m-%d %H:%M:%S')
    tmp_rf = loaddata[0, i, 0].astype(float)
    param_index = {"index": {"_type": "doc"}}
    param_data = {"time": tmp_time, "predict_rainfall": tmp_rf,"device":"B04"}
    action_body += json.dumps(param_index) + '\n'
    action_body += json.dumps(param_data) + '\n'
print(action_body)

result = es.bulk(body=action_body, index="predict_rainfall", doc_type="_doc")
print(result)