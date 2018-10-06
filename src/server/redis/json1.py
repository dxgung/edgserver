#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

import json
import random
import time
import redis
import threading

pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
r = redis.Redis(connection_pool=pool)
# r = redis.Redis(host='localhost', port=6379, db=0)
# d = 1537929674
# d = []
# msg = {}
# for i in range(5000):
#     d.append(random.random)
# data = {}
# for i in range(36):
#     name = str(i).rjust(3,"0") + "MT"
#     data[name] = d
# date_time = int(time.time())
# msg["DateTime"] = str(date_time)
# msg["Datas"] = data
# t = {"123":1, "345":4}
# for i in range(20):
#     date_time = int(time.time())
#     res = r.lpush(str(date_time), data)
#     time.sleep(1)
def high():
    before_start_time = None
    after_start_time = None
    in_test = False
    while True:
        try:
            high_msg = q.get()
        except:
            continue
        if in_test:
            t1 = threading.Thread(target=)
        # 如果首测启动高采
        if high_msg["db_name"] == "TestData" & in_test:
            pass
        # 如果是高存报警数据
        if high_msg["db_name"] == "AlarmData":
            # 情形1：如果首测启动高采.报警时间定义为t
            if not before_start_time:
                before_start_time = high_msg["DateTime"] - 10
                after_start_time = high_msg["DateTime"] + 30
            # 情形2：如果t < after_start_time
            elif (after_start_time - high_msg["DateTime"]) > 0 &  (after_start_time - high_msg["DateTime"]) < 30:
                before_start_time = after_start_time
                after_start_time = high_msg["DateTime"] + 30
            # 情形3：如果 0< t - aft1er_start_time < 10
            elif (after_start_time - high_msg["DateTime"]) < 0 & (after_start_time - high_msg["DateTime"]) > -10:
                before_start_time = after_start_time
                after_start_time = high_msg["DateTime"] + 30
            elif (after_start_time - high_msg["DateTime"]) <= -10:
                before_start_time = high_msg["DateTime"] - 10
                after_start_time = high_msg["DateTime"] + 30
            # 将高采数据送入指定的redis中，
            high_alarm_data(before_start_time, after_start_time)






start_time = q.get()  # 从队列中获取高存触发时间。
start_time = 1537931798
time_list = [1537931798, 1537931799, 1537931800]  # 时间列表，用于保存redis缓存中的时间信息
before_start_time = start_time+ 10  # 报警前10秒
after_start_time = start_time+ 30  # 报警后30秒
if before_start_time in time_list:

high_msg = {}
# 如果收到试验启动信号，并且启动信号为1
if high_msg["SourceName"] == "142XR" & high_msg["Status"] == 1 :
    high_test_data(high_msg)  # 执行试验过程高采
else:
    high_alarm_data(high_msg)  # 执行报警过程高采

print("done")

def high_alram_data(before_time, after_time, start_time):
    """
    报警过程高采
    :param high_msg:
    :return:
    """
    r = redis.Redis(host='localhost', port=6379, db=0)
    # 从缓存中将数据拿出，送入需要写入数据库的redis列表中。
    for i in range(before_time, after_time+1, 1):
        data = r.blpop(str(i), timeout=2)
        data_dict = eval(data)
        data_dict["AlarmTime"] = start_time
        if not data: # 如果数据非空
            r.rpush("AlarmData", data_dict)

def high_test_data(before_time, q):
    """
    试验过程高采
    :param before_time: 高采开始时间，精确到秒
    q：队列，用于传输循环停止指令
    :return:
    """
    r = redis.Redis(host='localhost', port=6379, db=0)
    start = None
    cmd = None
    # 从缓存中将数据拿出，送入需要写入数据库的redis列表中。
    #
    while cmd != "stop":
        try:
            cmd = q.get()
        except:
            continue
        if not start:
            start = before_time
        if time.time()-start > 3600*4:  # 如果高采超过4小时，停止
            cmd = "stop"
        date_time = before_time
        data = r.blpop(str(date_time), timeout=20)
        if not data: # 如果数据非空
            r.rpush("TestData", data)
        date_time += 1
