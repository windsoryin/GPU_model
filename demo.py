import threading
import queue
import time
from pandasticsearch import DataFrame
import pandas as pd
# pd.set_option('display.max_columns', None)
from datetime import datetime
import numpy as np

es_url = 'http://172.16.20.3:9200'
es_username = 'gnss'
es_password = 'YKY7csX#'
es_doc_type = 'doc'
es_index='B04'
# 分割时间
def split_time(start_timestamp, end_timestamp, range_duration):
    # 初始化起始时间和结束时间
    start_time = start_timestamp
    end_time = start_time + range_duration
    # 分割时间戳范围为多个范围
    ranges_list = []
    if end_time >= end_timestamp:
        ranges_list.append((start_time, end_timestamp))
        return ranges_list
    while end_time <= end_timestamp:
        ranges_list.append((start_time, end_time - 1))
        start_time = end_time
        end_time += range_duration
        if start_time < end_timestamp < end_time:
            ranges_list.append((start_time, end_timestamp))
    #ranges_list = np.linspace(start_timestamp, end_timestamp, 8)
    return ranges_list


# 请求数据
def req_worker(data, device, result_queue):
    # 处理数据
    df = DataFrame.from_es(url=es_url, index=es_index, username=es_username, password=es_password, verify_ssl=False,
                           compat=6)
    # 固定为doc
    df._doc_type = es_doc_type
    data = df.filter((df['timestamp'] >= data[0]) & (df['timestamp'] <= data[1]) & (df['device'] == device)) \
        .select(*df.columns) \
        .sort(df["timestamp"].desc) \
        .limit(10000) \
        .to_pandas()
    # 将结果加入队列
    result_queue.put(data)


# 合并结果
def req_queue(data, device):
    # 定义线程数量
    num_threads = len(data)
    # 创建线程池
    threads = []
    result_queues = []
    for i in range(num_threads):
        q = queue.Queue()
        t = threading.Thread(target=req_worker, args=(data[i], device, q))
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


if __name__ == '__main__':
    # 定义时间戳范围
    # start_timestamp = 1619011200  # 2021年4月21日00:00:00的时间戳（以UTC时区为准）
    # end_timestamp = 1619798401  # 2021年4月30日00:00:00的时间戳（以UTC时区为准）
    # 定义每个范围的持续时间（单位：秒）
    # range_duration = 86400  # 每个范围为24小时
    start_time = datetime(2023, 4, 17, 23, 0, 0)
    start_timestamp = start_time.timestamp()
    end_timestamp = start_timestamp + 3600 * 4
    range_duration = 1800
    device = 'B04'
    ranges_list = split_time(start_timestamp, end_timestamp, range_duration)
    # 输出结果
    for i in range(len(ranges_list)):
        print(f"Range {i + 1}: ({ranges_list[i][0]}, {ranges_list[i][1]})")
    # 记录开始时间
    b_time = time.time()
    # 多线程读取
    results = req_queue(ranges_list, device)
    print(results)
    # 记录结束时间
    e_time = time.time()
    print(e_time-b_time)
