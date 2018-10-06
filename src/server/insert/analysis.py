#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:
import queue
from utils import *
import pandas as pd
import pymongo
import redis
import time
import threading

def high_alram_data(before_time, after_time, start_time):
    """
    报警过程高采
    :param high_msg:
    :return:
    """
    r = redis.Redis(host='localhost', port=6379, db=0)
    # 从缓存中将数据拿出，送入需要写入数据库的redis有序集合中。
    for i in range(before_time, after_time+1, 1):
        data = r.blpop(str(i), timeout=20)
        if data:
            data_dict = eval(data)
            for key in data_dict:
                data_dict[key]["AlarmTime"] = start_time * 1000  # 将启动时间加到数据中，精确到毫秒
            r.zadd("alarm_data", data_dict, i)  # 将数据放入有序集合中，成员的值是时间戳，精确到秒


def high_test_data(start_time):
    """
    试验过程高采，将数据从缓存移入test_data有序集合
    :param before_time: 高采开始时间，精确到秒
    q：队列，用于传输循环停止指令
    :return:
    """
    r = redis.Redis(host='localhost', port=6379, db=0)
    start = None  # 记录高存开始的时间
    cmd = None
    stop_start = None
    is_stop = None
    # 从缓存中将数据拿出，送入需要写入数据库的redis列表中。
    #
    while is_stop != "stop":  # 如果外界发送stop命令，循环停止。如果001CC为低，循环就要停止

        # 接收外界发来的停止命令。
        cmd = str(r.get("cmd"), encoding="utf-8")
        if (cmd is "stop") & (not stop_start):
            stop_start = int(time.time())
        # 接收到停止命令后，再采集30秒后，停止
        if int(time.time()) - stop_start > 30:
            is_stop = "stop"
        if not start:
            start = start_time - 10
        # 如果高存超过4小时，停止
        if time.time()-start > 3600*4:  # 如果高采超过4小时，停止
            cmd = "stop"
        # 记录数据的时间，精确到秒
        date_time = start
        """
        test_data的格式如下：
        test_data = {"300MT":{
                      "DateTime": 1,  # 数据时间，精确到秒
                      "Frequency": 1,  # 频率，10或5000
                      "Data":[1,2,3,4]},  # 数据，存储1秒的数据
             "301MT":{}}
        """
        test_data = r.blpop(str(date_time), timeout=20)  # 从redis数据库中取出

        if not test_data: # 如果数据非空
            data_dict = eval(test_data)
            for key in data_dict:
                data_dict[key]["TestTime"] = start_time*1000  # 将启动时间加到数据中，精确到毫秒
            r.zadd("test_data", data_dict, date_time)  # 将数据放入有序集合中，成员的值是时间戳，精确到秒
        date_time += 1


def high_analysis(q):
    """
    分析是否需要高存，以及将高存数据从缓存中，挪入需要写入MongoDB的有序集合中
    :return:
    """

    r = redis.Redis(host='localhost', port=6379, db=0)

    before_start_time = None
    after_start_time = None
    in_test = False
    while True:
        try:
            high_msg = q.get()
        except:
            continue
        #  142XR是试验启动按钮
        if (high_msg["source_name"] is "142XR") & (high_msg["status"] == 0) & in_test:
            r.set("cmd", "stop")  # 停止high_test_data
        else:
            r.set("cmd", "start")
        # 如果是高存试验数据
        if high_msg["db_name"] is "TestData":
            t1 = threading.Thread(target=high_test_data, args=(high_msg["DateTime"],))
            t1.start()
            in_test = True

        # 如果在试验过程中，忽略所有的报警
        if not in_test:
            # 如果是高存报警数据
            if high_msg["db_name"] == "AlarmData":
                # 情形1：如果首测启动高采.报警时间定义为t
                if not before_start_time:
                    before_start_time = high_msg["DateTime"] - 10
                    after_start_time = high_msg["DateTime"] + 30
                # 情形2：如果t < after_start_time
                elif (after_start_time - high_msg["DateTime"]) > 0 & (after_start_time - high_msg["DateTime"]) < 30:
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
                high_alarm_data(before_start_time, after_start_time, high_msg["DateTime"])





def cmd_analysis(topic_name, cmd):
    """

    :param topic_name:
    :param cmd:
    :return:
    """
    # 解析从kafka发来的命令，送入查询函数


    cmds = dict()  # 命令
    conditions = []
    #  数据库信息：AlarmLog
    if topic_name == "HistoryAlarmCmd":
        database_name = "AlarmLog"
        if cmd["SourceName"] == "All":
            conditions = [{"StartTime": {"$lte": cmd["EndTime"]}, "EndTime": {"$gte": cmd["EndTime"]}},
                          {"StartTime": {"$lte": cmd["StartTime"]}, "EndTime": {"$gte": cmd["EndTime"]}},
                          {"StartTime": {"$gte": cmd["StartTime"]}, "EndTime": {"$gte": cmd["StartTime"]}}]
        else:
            conditions = [{"StartTime": {"$gte": cmd["StartTime"], "$lte": cmd["EndTime"]},
                           "AlarmSource": cmd["SourceName"]},
                          {"StartTime": {"$lte": cmd["StartTime"]},
                           "EndTime": {"$gte": cmd["EndTime"]},
                           "AlarmSource": cmd["SourceName"]},
                          {"EndTime": {"$gte": cmd["StartTime"], "$lte": cmd["EndTime"]},
                           "AlarmSource": cmd["SourceName"]}]

        # 返回查询列表
        cmds["topic_name"] = topic_name
        cmds["database_name"] = database_name
        cmds["source_name"] = cmd["SourceName"]
        cmds["conditions"] = conditions

    if topic_name == "HistoryDigitalChangeCmd":
        # 查找条件
        conditions = [{"DateTime": {"$gte": cmd["StartTime"], "$lte": cmd["EndTime"]}}]

        # 查询列表
        cmds["topic_name"] = topic_name
        cmds["database_name"] = "DigitalLog"
        cmds["source_name"] = cmd["SourceName"]
        cmds["conditions"] = conditions

    # 历史数据查询
    if topic_name == "HistoryCmd":
        # 查找条件
        # cmd:{"SourceName":["157MP","300MT","905XR"],
        #      "Time":[{"StartTime":1536835474000,"EndTime":1536835474000},
        #              {"StartTime":1536835474000,"EndTime":1536835474000}]}

        for i in range(len(cmd["Time"])):
            conditions.append([{"DateTime": {"$gte": cmd["Time"][0]["StartTime"],
                                             "$lte": cmd["Time"][0]["EndTime"]}}])

        # 查询列表
        cmds["topic_name"] = topic_name
        # 精确到秒
        search_time = int((cmd["Time"][0]["EndTime"] - cmd["Time"][0]["StartTime"])/1000)
        # 如果时间小于30秒，先判断是否有高存数据，如果有高存数据，将高存数据库送入cmds["database_name"]。
        # 如果没有高存，将OneSecondData送入cmds["database_name"]
        if search_time < 30:
            start_time = int(cmd["Time"][0]["StartTime"]/1000)
            end_time = int(cmd["Time"][0]["EndTime"]/1000)
            if in_high_storage(start_time, end_time):
                cmds["database_name"] = in_high_storage(start_time, end_time)
            else:
                cmds["database_name"] = "OneSecondData"

        # 如果在30秒~3小时之内，从OneSecondData读取数据
        if (search_time <= 10800) and (search_time > 30):
            cmds["database_name"] = "OneSecondData"
        # 如果在3小时~7天之内，从OneMinuteData读取数据
        if (search_time > 10800) and (search_time <= 604800):
            cmds["database_name"] = "OneMinuteData"
        # 如果大于7天，从TenMinutesData读取数据
        if search_time > 604800:
            cmds["database_name"] = "TenMinutesData"

        cmds["source_name"] = cmd["SourceName"]
        cmds["conditions"] = conditions

    # 试验信息查询
    if topic_name == "TestCmd":
        # 查询命令
        # {"TestCmd":"TestInfo"}
        cmds["database_name"] = "OtherData"
        cmds["source_name"] = "TestTime"

    return cmds


