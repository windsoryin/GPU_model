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
import calendar
import schedule
import time
from math import *
import threading
import queue

# pandas打印所有列，生产环境可以去掉
pd.set_option('display.max_columns', None)
"""
系统不在提供外网的IP的服务，全部以域名访问, 完成测试后需要把安全组中不需要的端口全部删除

1、CTSDB 外网访问地址变动：
http://42.193.247.49:9200
修改为：
http://ctsdb.gnss.accurain.cn
内网地址不变（http://172.16.20.3:9200）

2、网站域名增加SSL证书
https://www.accurain.cn/

3、Python的API外网地址
https://data-api.gnss.accurain.cn

4、NGINX配置地址：
https://nginx.gnss.accurain.cn
账号：admin@accurain.cn
密码：YKY7csX#

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

global seq_len
global site_name


# 分割时间
def split_time(start_timestamp, end_timestamp):
    #range_duration=60*60
    # 默认分为八段
    ranges_list = np.linspace(start_timestamp, end_timestamp, 8)
    return ranges_list


# 请求数据
def req_worker(start_t, end_t, device, result_queue, es_index):
    es_url = 'http://172.16.20.3:9200'
    es_username = 'gnss'
    es_password = 'YKY7csX#'
    es_doc_type = 'doc'
    # 处理数据
    df = DataFrame.from_es(url=es_url, index=es_index, username=es_username, password=es_password, verify_ssl=False,
                           compat=6)
    # 固定为doc
    df._doc_type = es_doc_type
    data = df.filter((df['timestamp'] >= start_t) & (df['timestamp'] <= end_t) & (df['device'] == device)) \
        .select(*df.columns) \
        .sort(df["timestamp"].asc) \
        .limit(10000) \
        .to_pandas()
    # 将结果加入队列
    result_queue.put(data)


# 合并结果
def req_queue(rangelist, device, index_name):
    # 定义线程数量
    num_threads = len(rangelist)-1
    # 创建线程池
    threads = []
    result_queues = []
    for i in range(num_threads):
        q = queue.Queue()
        t = threading.Thread(target=req_worker, args=(rangelist[i], rangelist[i+1], device, q, index_name))
        threads.append(t)
        result_queues.append(q)

    # 启动线程
    for t in threads:
        t.start()

    # 等待所有线程结束
    for t in threads:
        t.join()

    # 合并结果
    results = []
    for q in result_queues:
        while not q.empty():
            results.append(q.get())

    # 返回结果
    return pd.concat(results)

def read_database(index_name, start_t, end_t, site_name):
    # index_name 数据库名: gnss@* ; hws@*
    # start_t,end_t: 开始和结束时间戳
    # range_duration: 多线程，时间段
    # site_name: 读取设备号
    ranges_list = split_time(start_t, end_t)
    # 输出结果
    for i in range(len(ranges_list)-1):
        print(f"Range {i + 1}: ({ranges_list[i]}, {ranges_list[i+1]})")
    # 记录开始时间
    b_time = time.time()
    # 多线程读取
    data = req_queue(ranges_list, site_name, index_name)

    #print(results)
    # 记录结束时间
    e_time = time.time()
    print(e_time-b_time)
    return data


def dynamic_window(winsize, win_max, gnss_data, hws_data, resample_time):
    flag = 1
    while flag:
        t1 = resample_time - winsize  # i*5*60 5min interval；  【-0.5*60，0.5*60】window length
        t2 = resample_time + winsize
        tmp_data = gnss_data[(gnss_data['timestamp'] > int(t1)) & (gnss_data['timestamp'] < int(t2))]
        mean_data = tmp_data.loc[:, ['ztd', 'latitude', 'longitude', 'height']].mean(0)
        tmp_hwsdata = hws_data[(hws_data['timestamp'] > int(t1)) & (hws_data['timestamp'] < int(t2))]
        mean_hwsdata = (tmp_hwsdata.loc[:, ['Ta', 'Pa', 'Ta']].mean(0))
        winsize = winsize * 2
        if (tmp_data.shape[0]) >= 1 & tmp_hwsdata.shape[0] >= 1:
            flag = 0
        if winsize > win_max:
            flag = 0
    t1 = resample_time - 2.5 * 60  # i*5*60 5min interval；  【-0.5*60，0.5*60】window length
    t2 = resample_time + 2.5 * 60
    tmp_hwsdata = hws_data[(hws_data['timestamp'] > int(t1)) & (hws_data['timestamp'] < int(t2))]
    Rc_data = tmp_hwsdata['Rc'].values
    Rc_diff = Rc_data[-1] - Rc_data[0]
    mean_hwsdata['Rc'] = Rc_diff
    return mean_data, mean_hwsdata


def resampling(now_time, gnss_data, hws_data, winsize, win_max):
    # resample real_data and interpolate those missing values
    now_time_utc = pd.to_datetime(now_time, unit='s')
    near_minute = np.floor(now_time_utc.minute / 5) * 5
    end_time_utc = now_time_utc.replace(minute=near_minute.astype(int), second=0, microsecond=0)
    start_time_utc = end_time_utc - datetime.timedelta(minutes=seq_len * 5)  # 根据seqlen=96，得到起始时间
    # resample_time = pd.date_range(start=start_time_utc, end=end_time_utc, freq='5min') # 产生重采样时间点集合
    end_time_unix = calendar.timegm(end_time_utc.timetuple())
    resample_time = calendar.timegm(start_time_utc.timetuple())
    resamp_data = pd.DataFrame(None,
                               index=['t2m(k)', 'sp(Pa)', 'd2m(k)', 'tp', 'ztd', 'latitude', 'longitude', 'height'])

    for i in range(seq_len):
        re_time = resample_time + (i) * 5 * 60
        # 动态改变窗口大小，获取时间窗口内平均值
        mean_data, mean_hwsdata = dynamic_window(winsize, win_max, gnss_data, hws_data, re_time)
        # 拼接数据
        s = pd.concat([mean_hwsdata, mean_data], axis=0).to_frame()
        s.index = ['t2m(k)', 'sp(Pa)', 'd2m(k)', 'tp', 'ztd', 'latitude', 'longitude', 'height']
        resamp_data = pd.concat([resamp_data, s], axis=1, ignore_index=True)

    time_dt = pd.DataFrame(np.arange(resample_time, end_time_unix, 5 * 60))
    resamp_data.loc[resamp_data.shape[0]] = np.arange(resample_time, end_time_unix, 5 * 60)
    resamp_data = resamp_data.rename(index={resamp_data.shape[0] - 1: 'date'})
    # resamp_data.drop(columns=0)
    resamp_data = resamp_data.T
    resamp_data['date'] = pd.to_datetime(resamp_data['date'], unit='s')
    return resamp_data


def calc_pwv(ztd, t, p, lat, height):
    # t: temperature (k)
    # p: pressure (hpa)
    # lat: latitude (degree)
    # height: (m)
    # ztd: zenith tropospheric delay (m)
    # Saastamoinen model
    tm = 70.2 + 0.72 * t
    lat = lat / 180 * pi
    zhd0 = pow(10, -3) * (2.2768 * p / (1 - 0.00266 * np.cos(2 * lat) - 0.00028 * height * pow(10, -3)))  # 单位m
    zwd0 = pow(10, 3) * (ztd - zhd0)  # %单位mm
    k = pow(10, 6) / (4.613 * pow(10, 6) * (3.776 * pow(10, 5) / tm + 22.1))  # 单位Mkg/m^3
    k = k * pow(10, 6) / pow(10, 3)  # 单位换算kg/m^2=mm
    pwv = k * zwd0
    return pwv


def get_data(now_time, history_time, site_name):
    gnss = 'gnss@*'
    hws = 'hws@*'
    end_t = now_time
    start_t = history_time
    gnss_data = read_database(gnss, start_t, end_t, site_name)
    hws_data = read_database(hws, start_t, end_t,site_name)
    if len(gnss_data)==0 or len(hws_data) == 0:
        flag=-1
        return flag

    gnss_data['timestamp'] = gnss_data['timestamp'].astype(int)
    gnss_data['ztd'] = gnss_data['ztd'].astype(float)
    gnss_data['latitude'] = gnss_data['latitude'].astype(float)
    gnss_data['longitude'] = gnss_data['longitude'].astype(float)
    gnss_data['height'] = gnss_data['height'].astype(float)
    gnss_data['time'] = pd.to_datetime(gnss_data['time'], unit='s')
    hws_data['time'] = pd.to_datetime(hws_data['time'], unit='s')
    hws_data['timestamp'] = hws_data['timestamp'].astype(int)
    hws_data['Ta'] = hws_data['Ta'].astype(float)
    hws_data['Pa'] = hws_data['Pa'].astype(float)
    hws_data['Rc'] = hws_data['Rc'].astype(float)
    hws_data['Ua'] = hws_data['Ua'].astype(float)
    # hws_data['time'] = pd.to_datetime(hws_data['time'], unit='s')
    # print(real_data)
    ##############
    resamp_data = resampling(now_time, gnss_data, hws_data, 30, 30 * 6)  # 重采样数据
    """
    读取数据，写入csv文件中
    """
    resamp_data.iloc[:, 0:8] = resamp_data.iloc[:, 0:8].interpolate(method='linear', order=1, limit=10,
                                                                    limit_direction='both')  #
    resamp_data["t2m(k)"] = resamp_data["t2m(k)"].astype(float)
    resamp_data["t2m(k)"] = resamp_data[["t2m(k)"]].apply(lambda x: x["t2m(k)"] + 273.15, axis=1)
    resamp_data["d2m(k)"] = resamp_data["d2m(k)"].astype(float)
    resamp_data["d2m(k)"] = resamp_data[["d2m(k)"]].apply(lambda x: x["d2m(k)"] + 274, axis=1)
    resamp_data["sp(Pa)"] = resamp_data["sp(Pa)"].astype(float)
    resamp_data["tp"] = resamp_data["tp"].astype(float)
    resamp_data["tp"] = resamp_data[["tp"]].apply(lambda x: x["tp"] + 0.0001, axis=1) # avoid nan when all zeros

# calculate PWV
    ztd_data = resamp_data.loc[:, 'ztd']
    t_data = resamp_data.loc[:, 't2m(k)']
    p_data = resamp_data.loc[:, 'sp(Pa)']
    lat_data = resamp_data.loc[:, 'latitude']
    h_data = resamp_data.loc[:, 'height']
    pwv = calc_pwv(ztd_data, t_data, p_data, lat_data, h_data)
    #
    data_csv = resamp_data[['date', 't2m(k)', 'sp(Pa)', 'd2m(k)']]  # 重组数据
    data_csv.insert(4, 'pwv', pwv)
    data_csv.insert(5, 'tp', resamp_data.loc[:, 'tp'])  # 重组数据

    data_csv.to_csv('./real_data/{}.csv'.format(site_name), index=False)
    print('done')
    flag = 1
    return flag

def run_model():
    # 通过subprocess.popen 执行 命令行命令
    p = subprocess.Popen(['python test02.py'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in p.stdout.readlines():
        print(line)
    retval = p.wait()


def write_database(now_time, site_name):
    now_time_utc = pd.to_datetime(now_time, unit='s')
    near_minute = np.floor(now_time_utc.minute / 5) * 5
    end_time_utc = now_time_utc.replace(minute=near_minute.astype(int), second=0, microsecond=0)
    end_time_unix = calendar.timegm(end_time_utc.timetuple())
    
    loaddata = np.load(
        './results/informer_ftMS_sl96_ll48_pl144_dm512_nh8_el2_dl1_df2048_atprob_fc5_ebtimeF_dtTrue_mxTrue_test_0/real_prediction_{}.npy'.format(
            site_name))
    data_log = pd.DataFrame(loaddata[:,:,0], index=['prediction_rainfall'])
    data_log.insert(data_log.shape[1], 'time', pd.to_datetime(now_time, unit='s'))
    data_log.to_csv('./log/prediction_log.csv', mode='a')
    es = Elasticsearch(hosts=["172.16.20.3:9200"], http_auth=('gnss', 'YKY7csX#'),
                       scheme="http")
    action_body = ''
    for i in range(loaddata.shape[1]):
        tmp_time = pd.to_datetime(end_time_unix+(i+1)*5*60, unit='s').strftime('%Y-%m-%d %H:%M:%S')
        tmp_rf = loaddata[0, i, 0].astype(float)
        param_index = {"index": {}} #"_type": "doc"
        param_data = {"time": tmp_time, "predict_rainfall": tmp_rf,"device":site_name}
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


# 定时系统
def job():  # 定时任务
    print("I'm working...")
    now_time = time.time()-3*3600
    time_interval = seq_len / 12 * 60 * 60  # 8小时的历史数据 = seqlen=96
    history_time = now_time - time_interval - 10*60  # 8小时的历史数据 = seqlen=96; 10*60: 预留空间
    site_name = "B06"  #读取xx站点的数据库
    print(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(history_time)))  # 当前时间
    flag = get_data(now_time, history_time, site_name)
    if flag==1:
        run_model()
        write_database(now_time, site_name)
        print('Successfully Done')
    else:
        print('No valid data')


schedule.every().hour.at('00:00').do(job)  # 在每小时的00分00秒开始，定时任务job
schedule.every().hour.at('30:00').do(job)  # 在每小时的00分00秒开始，定时任务job
#schedule.every().hour.at('40:00').do(job)  # 在每小时的00分00秒开始，定时任务job
#schedule.every().hour.at('37:00').do(job)  # 在每小时的00分00秒开始，定时任务job
schedule.every(10).seconds.do(job)
seq_len = 96
count_n = 60
while True:
    schedule.run_pending()
    time.sleep(1)
    if count_n==60:
      print('waiting')
      count_n=1
    count_n = count_n + 1

