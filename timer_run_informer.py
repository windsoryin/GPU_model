from pandasticsearch import DataFrame
import pandas as pd
import numpy as np
import time
import csv
import datetime
import os
import subprocess
import json
from elasticsearch6 import Elasticsearch

# pandas打印所有列，生产环境可以去掉
pd.set_option('display.max_columns', None)
"""
url：
外网地址:http://42.193.247.49:9200   内网地址: http://172.16.20.3:9200

index：
目前已经调试通过2个
1、HWS气象数据：hws@*
2、GNSS数据：gnss@*

username：
账号：gnss

password：
密码：YKY7csX#

verify_ssl：校验SSL证书 False

compat：兼容elasticsearch版本为6.8.2
"""

# 定时系统
import schedule
import time
def job():   # 定时任务
    print("I'm working...")
    now_time=time.time()
    time_interval=1*60*60 # 6小时的历史数据
    history_time=now_time-time_interval
    print(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(history_time)))# 当前时间
    get_data(now_time, history_time)
    run_model()
    write_database()
    print('successfully done')

def read_database(index_name,start_t,end_t):
    # 读取数据库数据
    # index_name：
    # 1、HWS气象数据：hws @ *
    # 2、GNSS数据：gnss @ *
    df = DataFrame.from_es(url='http://42.193.247.49:9200', index=index_name,
                           username="gnss", password="YKY7csX#", verify_ssl=False,
                           compat=6)
    # 固定为doc
    df._doc_type = "doc"

    # 打印schema 方便查看当前数据格式，生产环境去掉，
    # 目前数据格式中除了原本设备数据 还包含 两个时间字段 time设备数据字段  timestamp时序库时间字段（实际查询建议用此字段）。
    df.print_schema()

    # 查询事例  1、filter：时间过滤、设备过滤； 2、select查询所有字段  3、sort以什么字段进行排序  4、限制返回数量
    # 其他用法参考：https://github.com/onesuper/pandasticsearch
    data = df.filter((df['timestamp'] > start_t) & (df['timestamp'] < end_t) & (df['device'] == 'test'))\
        .select(*df.columns)\
        .sort(df["timestamp"].desc)\
        .limit(1000)\
        .to_pandas()
    return data

def get_data(now_time,history_time):
    gnss = 'gnss@*'
    hws = 'hws@*'
    end_t = now_time
    start_t = history_time
    gnss_data = read_database(gnss, start_t, end_t)
    gnss_data_invert=gnss_data.sort_values(by='timestamp',ascending=True,axis=0)
    ztd_data=gnss_data.loc[:,'ztd']
    hws_data = read_database(hws, start_t, end_t)
    hws_data['time'] = pd.to_datetime(hws_data['time'], unit='s')
    hws_data_invert = hws_data.sort_values(by='timestamp', ascending=True, axis=0)
    # print(data)
    """
    读取数据，写入csv文件中
    """
    data_csv=hws_data_invert[['time','Ta','Pa','Ta']]  #重组数据
    data_csv.insert(4, 'ztd', ztd_data)
    data_csv.insert(5, 'tp', hws_data.loc[:,'Rc'])     #重组数据
    data_csv.columns=['date','t2m(k)','sp(Pa)','d2m(k)','pwv','tp']  #修改列名
    data_csv["t2m(k)"] = data_csv["t2m(k)"].astype(float)
    data_csv["t2m(k)"] = data_csv[["t2m(k)"]].apply(lambda x: x["t2m(k)"] + 273, axis=1)
    data_csv["d2m(k)"] = data_csv["d2m(k)"].astype(float)
    data_csv["d2m(k)"] = data_csv[["d2m(k)"]].apply(lambda x: x["d2m(k)"] + 274, axis=1)
    data_csv["sp(Pa)"] = data_csv["sp(Pa)"].astype(float)
    data_csv["sp(Pa)"] = data_csv[["sp(Pa)"]].apply(lambda x: x["sp(Pa)"]*100, axis=1) # unit from hpa to pa

    #data_csv.to_csv('./real_data/tp.csv', index=False)
    print('done')

def run_model():
    p = subprocess.Popen(['python test02.py'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in p.stdout.readlines():
        print(line)
    retval = p.wait()

def write_database():
    loaddata = np.load(
        './results/informer_ftMS_sl96_ll48_pl24_dm512_nh8_el2_dl1_df2048_atprob_fc5_ebtimeF_dtTrue_mxTrue_test_0/real_prediction_tp')

    es = Elasticsearch(hosts=["42.193.247.49:9200"], http_auth=('gnss', 'YKY7csX#'),
                       scheme="http")
    action_body = ''
    for i in range(len(loaddata)):
        tmp_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        tmp_rf = loaddata[0, i, 0].astype(float)
        param_index = {"index": {"_type": "doc"}}
        param_data = {"time": tmp_time, "predict_rainfall": tmp_rf}
        action_body += json.dumps(param_index) + '\n'
        action_body += json.dumps(param_data) + '\n'
    print(action_body)

#schedule.every().hour.at('55:05').do(job)  # 在每小时的00分00秒开始，定时任务job
schedule.every(5).seconds.do(job)
while True:
    schedule.run_pending()
    time.sleep(1)



