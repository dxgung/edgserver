#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

from confluent_kafka import Producer
import pymongo
import json
import queue
import time
import sys

# from utils import simulate_producer
import settings

def simulate_cmd(cmd_topic):
    """

    CMD_TOPICS = ["HistoryAlarmCmd",  # 历史报警信息查询
                 "History300CRAlarmCmd",  # 300CR历史报警信息查询
                 "HistoryDigitalChangeCmd",  # 开关量变化信息查询
                 "HistoryCmd",  # 历史数据查询
                 "TestCmd"]  # 试验信息查询

    :param q1:
    :return:
    """
    broker = settings.BROKER
    topics = settings.CMD_TOPICS
    # 配置Producer
    conf = {'bootstrap.servers': broker}

    # 创建Producer实例
    p = Producer(**conf)

    if cmd_topic == "HistoryAlarmCmd":
        cmd = {"SourceName": "300MT",
               "StartTime": 1537668000000,  # 2018/9/23 10:00:00
               "EndTime": 1537668000000}  # 2018/9/23 10:00:00
        # cmd = {"SourceName": "All",
        #        "StartTime": 1537668000000,  # 2018/9/23 10:00:00
        #        "EndTime": 1537668000000}  # 2018/9/23 10:00:00
        j_cmd = json.dumps(cmd)
        p.produce(cmd_topic, j_cmd)  # 发送到kafka

    if cmd_topic == "History300CRAlarmCmd":
        cmd = {"SourceName": "300MT",
               "StartTime": 1537668000000,  # 2018/9/23 10:00:00
               "EndTime": 1537668000000}  # 2018/9/23 10:00:00
        # cmd = {"SourceName": "All",
        #        "StartTime": 1537668000000,  # 2018/9/23 10:00:00
        #        "EndTime": 1537668000000}  # 2018/9/23 10:00:00
        j_cmd = json.dumps(cmd)
        p.produce(cmd_topic, j_cmd)  # 发送到kafka

    if cmd_topic == "HistoryDigitalChangeCmd":
        cmd = {"SourceName": "905XR",
               "StartTime": 1537668000000,  # 2018/9/23 10:00:00
               "EndTime": 1537668000000}  # 2018/9/23 10:00:00
        # cmd = {"SourceName": "All",
        #        "StartTime": 1537668000000,  # 2018/9/23 10:00:00
        #        "EndTime": 1537668000000}  # 2018/9/23 10:00:00
        j_cmd = json.dumps(cmd)
        p.produce(cmd_topic, j_cmd)  # 发送到kafka

    if cmd_topic == "HistoryCmd":
        # 单时间
        cmd = {"SourceName": ["905XR", "906XR", "907XR"],
               "Time": [{"StartTime": 1537668000000, "EndTime": 1537668000000}]
               }
        # 双时间
        # cmd = {"SourceName": ["905XR", "906XR", "907XR"],
        #        "Time": [{"StartTime": 1537668000000, "EndTime": 1537668000000},
        #                 {"StartTime": 1537668000000, "EndTime": 1537668000000}]
        #        }
        j_cmd = json.dumps(cmd)
        p.produce(cmd_topic, j_cmd)  # 发送到kafka

    if cmd_topic == "TestCmd":
        # 单时间
        cmd = {"TestCmd": "TestInfo"}

        j_cmd = json.dumps(cmd)
        p.produce(cmd_topic, j_cmd)  # 发送到kafka

    p.poll(0)
    p.flush()

def sim():
    """
    模拟producer
    :return:
    """
    topics = settings.CMD_TOPICS
    for topic in topics:
        simulate_cmd(topic)

def find(cmd_topic, cmd):
    """
    根据从kafka获取的查询命令，从数据库查询数据，并将返回查询结果
    :param cmd_topic:
    :param cmd:
    :return: 返回查询结果，
    """

    msg = None
    # 历史报警信息查询
    if cmd_topic == "HistoryAlarmCmd":
        msg = find_history_alarm(cmd)

    # 300CR历史报警信息查询
    if cmd_topic == "History300CRAlarmCmd":
        msg = find_history_300cr_alarm(cmd)

    # 开关量变化信息查询
    if cmd_topic == "HistoryDigitalChangeCmd":
        msg = find_history_digital_change(cmd)

    # 历史数据查询
    if cmd_topic == "HistoryCmd":
        msg = find_history(cmd)

    # 试验信息查询
    if cmd_topic == "TestCmd":
        msg = find_test(cmd)

    return msg

def find_history_alarm(cmd):
    """

    :param cmd:
    {"SourceName":"300MT","StartTime":153680864000,"EndTime":153680864000}
    {" SourceName ":"All","StartTime":153680864000,"EndTime":153680864000}
    :return:msg
    {"DateTime": 1537667921000,
     "topic": "History300CRAlarm",  # python内部使用，不发给kafka
     "ListDatas":[]}
    """
    # 从MongoDB查询历史报警信息HistoryAlarm数据

    # 解析查询命令
    cmds = parse_history_alarm(cmd)

    msg = dict()  # 返回给kafka的数据
    msg["DateTime"] = int(time.time()) * 1000
    msg["topic"] = "HistoryAlarm"
    msg["ListDatas"] = list()

    client = pymongo.MongoClient()
    db = client["AlarmLog"]
    col = db["MainAlarm"]
    results = list()
    for condition in cmds["conditions"]:
        docs = col.find(condition, {"_id": 0, "AlarmDescription":0}).sort("StartTime")
        for doc in docs:
            results.append(doc)


    # 将查询结果转换为预定义的kafka消息格式
    # 预定义消息格式如下
    """
    按时间查询
    {
        "DateTime":1537514520, 
        "ListDatas":[{"SourceName":"300MT", "List":[{"AlarmValue":1,"EndTime":1537514520},{"AlarmValue":1,"EndTime":1537514520}]},
                     {"SourceName":"301MT", "List":[{"AlarmValue":1,"EndTime":1537514520}]}]
    }
    按测点查询
    {
        "DateTime":1537514520, 
        "ListDatas":[{"SourceName":"300MT", "List":[{"AlarmValue":1,"EndTime":1537514520},{"AlarmValue":1,"EndTime":1537514520}]}]
    }
    """
    if len(results) != 0:  # 如果查询到结果，对查询结果进行解析
        source_names = set()  # 集合，用于存储报警源
        if cmds["SourceName"] == "All":  # 按时间查询
            for result in results:  # 获取报警源（不重复）
                source_names.add(result["AlarmSource"])
            for source_name in source_names:  # 根据报警源分类
                tmp = dict()
                tmp["SourceName"] = source_name
                tmp["List"] = list()
                index = 0
                for result in results:
                    if result["SourceName"] == source_name:
                        del result["AlarmSource"]  # 将报警源从字典中删除
                        tmp["List"].append(result)
                        del results[index]  # 将记录从列表中删除
                    index += 1
                msg["ListDatas"].append(tmp)
        else:  # 按测点查询
            tmp = dict()
            tmp["SourceName"] = cmds["SourceName"]
            tmp["List"] = list()
            for result in results:
                del result["AlarmSource"]
                tmp["List"].append(result)
            msg["ListDatas"].append(tmp)

    return msg

def find_history_300cr_alarm(cmd):
    """

    :param cmd: 查询命令
    {"SourceName":"110XR","StartTime":153680864000,"EndTime":153680864000}
    {"SourceName":"All","StartTime":153680864000,"EndTime":153680864000}

    :return:msg

    """

    # 解析查询命令
    cmds = parse_history_alarm(cmd)

    msg = dict()  # 返回给kafka的数据
    msg["DateTime"] = int(time.time()) * 1000
    msg["topic"] = "History300CRAlarm"
    msg["ListDatas"] = list()

    client = pymongo.MongoClient()
    db = client["300CRAlarmLog"]
    col = db["MainAlarm"]
    results = list()
    for condition in cmds["conditions"]:
        docs = col.find(condition, {"_id": 0}).sort("StartTime")
        for doc in docs:
            results.append(doc)



    # 将查询结果转换为预定义的kafka消息格式
    # 预定义消息格式如下
    """
    按时间查询
    {
        "DateTime":1537514520, 
        "ListDatas":[{"SourceName":"300MT", "List":[{"AlarmValue":1,"EndTime":1537514520},{"AlarmValue":1,"EndTime":1537514520}]},
                     {"SourceName":"301MT", "List":[{"AlarmValue":1,"EndTime":1537514520}]}]
    }
    按测点查询
    {
        "DateTime":1537514520, 
        "ListDatas":[{"SourceName":"300MT", "List":[{"AlarmValue":1,"EndTime":1537514520},{"AlarmValue":1,"EndTime":1537514520}]}]
    }
    """
    if len(results) != 0:
        source_names = set()  # 集合，用于存储报警源
        if cmds["SourceName"] == "All":  # 按时间查询
            for result in results:  # 获取报警源（不重复）
                source_names.add(result["AlarmSource"])
            for source_name in source_names:  # 根据报警源分类
                tmp = dict()
                tmp["SourceName"] = source_name
                tmp["List"] = list()
                index = 0
                for result in results:
                    if result["SourceName"] == source_name:
                        del result["AlarmSource"]  # 将报警源从字典中删除
                        tmp["List"].append(result)
                        del results[index]  # 将记录从列表中删除
                    index += 1
                msg["ListDatas"].append(tmp)
        else:
            tmp = dict()
            tmp["SourceName"] = cmds["SourceName"]
            tmp["List"] = list()
            for result in results:
                del result["AlarmSource"]
                tmp["List"].append(result)
            msg["ListDatas"].append(tmp)

    return msg

def find_history_digital_change(cmd):
    """
    从MongoDB查询开关量变化信息history digital change数据，返回kafka预定义的topic
    :param cmd
    {"SourceName":"905XR","StartTime":153680864000,"EndTime":153680864000}
    {"SourceName":"All","StartTime":153680864000,"EndTime":153680864000}


    :return: msg：可以写入kafka的查询结果
    """

    # 将查询结果转换为预定义的kafka消息格式
    # 预定义消息格式如下
    """
    按时间查询
    {
        "DateTime":1537514520, 
        "ListDatas":[{"SourceName":"300MT", "List":[{"DateTime":1536747660379,"Status":0},{"DateTime":1536747660379,"Status":0}]},
                     {"SourceName":"301MT", "List":[{"DateTime":1536747660379,"Status":0}]}]
    }
    按测点查询
    {
        "DateTime":1537514520, 
        "ListDatas":[{"SourceName":"300MT", "List":[{"DateTime":1536747660379,"Status":0},{"DateTime":1536747660379,"Status":0}]}]
    }
    """
    #  解析查询命令
    cmds = parse_history_digital_change(cmd)

    msg = {}  # 返回给kafka的数据
    msg["DateTime"] = int(time.time())*1000
    msg["topic"] = "HistoryDigitalChange"
    msg["ListDatas"] = list()

    # 初始化数据库连接
    client = pymongo.MongoClient()
    db = client["DigitalLog"]

    if cmds["source_name"] == "All":  # 按时间查询
        for col_name in db.collection_names():  # 获取数据库下所有的集合
            if col_name != "system.indexes":
                result = {}
                result["List"] = []
                col = db[col_name]
                for condition in cmds["conditions"]:
                    docs = col.find(condition, {"_id": 0}).sort("DateTime")  # cmds["conditions"]只有一个元素
                    for doc in docs:
                        result["List"].append(doc)
                result["SourceName"] = col_name
            if len(result) != 0:
                msg["ListDatas"].append(result)
    else:  # 按测点查询
        col = db[cmds["source_name"]]  # 连接指定集合
        result = {}
        result["List"] = []
        result["SourceName"] = cmds["source_name"]
        for condition in cmds["conditions"]:
            docs = col.find(condition, {"_id": 0}).sort("DateTime")  # cmds["conditions"]只有一个元素
            for doc in docs:
                result["List"].append(doc)
        if len(result) != 0:
            msg["ListDatas"].append(result)

    client.close()

    return msg

def find_history(cmd):
    """
    从MongoDB查询历史数据，返回kafka预定义的topic
    :param cmds:
            cmds = {}
            conditions = [{"DateTime": {"$gte": 1537579099096, "$lte": 1537582949212}}]
            cmds["topic_name"] = "HistoryCmd"
            cmds["database_name"] = "DigitalLog"
            cmds["source_name"] = ["300MT", "301MT", "302MT"]
            cmds["conditions"] = conditions
    :return: msg：可以写入kafka的查询结果
    """

    # 将查询结果转换为预定义的kafka消息格式
    # 预定义消息格式如下
    """
    {"datetime":1537010295037,
    "listDatas":[{"SourceName":"003ID",
                  "StartTime":1537009379000, # 数据起始时间，精确到秒，然后乘以1000，到毫秒
                  "Frequency":1,  # 频率。如果是秒级数据=1，如果是分钟级数据=1/60，如果是10分钟级数据=1/600
                  "Data":[1,1,1,1,1], 
                  "DigitalLog":[]}]  # 如果查询的高存数据中，有数字量变化，返回数字量变化情况

    """

    cmds = parse_history(cmd)
    msg = {}  # 返回给kafka的数据
    msg["DateTime"] = int(time.time())*1000
    msg["topic"] = "HistoryData"
    msg["ListDatas"] = list()

    # 初始化数据库连接
    client = pymongo.MongoClient()
    db = client[cmds["database_name"]]

    if db == "OneSecondData":
        for source_name in cmds["source_name"]:
            result = {}
            col = db[source_name]
            result = find_one_second_data(col, cmd["start_time"], cmd["end_time"])
            if len(result) != 0:
                msg["ListDatas"].append(result)

    if db =="OneMinuteData":
        for source_name in cmds["source_name"]:
            result = {}
            col = db[source_name]
            result = find_one_minute_data(col, cmd["start_time"], cmd["end_time"])
            if len(result) != 0:
                msg["ListDatas"].append(result)

    if db =="TenMinutesData":
        for source_name in cmds["source_name"]:
            result = {}
            col = db[source_name]
            result = find_ten_minutes_data(col, cmd["start_time"], cmd["end_time"])
            if len(result) != 0:
                msg["ListDatas"].append(result)

    if db == "AlarmData":
        for source_name in cmds["source_name"]:
            result = {}
            col = db[source_name]
            result = find_alarm_data(col, cmd["start_time"], cmd["end_time"])
            if len(result) != 0:
                msg["ListDatas"].append(result)

    if db == "TestData":
        for source_name in cmds["source_name"]:
            result = {}
            col = db[source_name]
            result = find_test_data(col, cmd["start_time"], cmd["end_time"])
            if len(result) != 0:
                msg["ListDatas"].append(result)



    client.close()  # 关闭数据库连接
    #  消息大小，精确到MB
    msg_size = sys.getsizeof(msg)/1024/1024

    if msg_size > 8:
        msg["DateTime"] = int(time.time())*1000
        msg["Message"] = "Large Message Size"

    return msg

def find_test(cmd):
    """
    从MongoDB读取试验信息，返回给kafka预定义的topic：TestInfo
    :param cmds:
            cmds["database_name"] = "OtherData"
            cmds["source_name"] = "TestTime"
    :return: msg:可以写入kafka的查询结果
            {"DataTime": 1537622741000,
             "ListDatas":[{startupTime":1536993787000,"stopTime":1536993808000,"testTime":1536993780000},
                          {startupTime":1536993787000,"stopTime":1536993808000,"testTime":1536993780000}
             ]}

    """
    #  查询命令解析
    cmds = parse_test(cmd)
    msg = {}
    msg["DateTime"] = int(time.time()) * 1000
    msg["topic"] = "TestInfo"
    msg["ListDatas"] = []

    client = pymongo.MongoClient()
    db = client[cmds["database_name"]]
    col = db["source_name"]

    docs = col.find({},{"_id":0}).sort("TestTime")
    for doc in docs:
        msg["ListDatas"].append(doc)

    client.close()

    return msg

def parse_history(cmd):
    """
    解析历史数据查询指令
    :param cmd:
    cmd:{"SourceName":["157MP","300MT","905XR"],
         "Time":[{"StartTime":1536835474000,"EndTime":1536835474000},
                 {"StartTime":1536835474000,"EndTime":1536835474000}]}
    :return:
    """
    cmds = {}
    conditions = []
    for i in range(len(cmd["Time"])):
        conditions.append([{"DateTime": {"$gte": cmd["Time"][0]["StartTime"],
                                         "$lte": cmd["Time"][0]["EndTime"]}}])

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
    return cmds


def parse_test(cmd):
    """
    解析查询命令
    :param cmd:
    :return:
    """
    cmds = {}
    cmds["database_name"] = "OtherData"
    cmds["source_name"] = "TestTime"

    return cmds

def parse_history_alarm(cmd):
    """
    解析历史报警 HistoryAlarmCmd 查询命令
    :param cmd:
    {"SourceName":"300MT","StartTime":153680864000,"EndTime":153680864000}
    {"SourceName ":"All","StartTime":153680864000,"EndTime":153680864000}
    :return:cmds
    {"source_name": "300MT" / "All",
     "conditions":}
    """
    cmds = {}

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
    cmds["source_name"] = cmd["SourceName"]
    cmds["conditions"] = conditions

    return cmds

def parse_history_digital_change(cmd):
    """
    解析查询命令
    :param cmd:
    {"SourceName":"905XR","StartTime":153680864000,"EndTime":153680864000}
    :return:cmds
    cmds =  "source_name": "531XR" / "All"
            "conditions": [{"DateTime": {"$gte": 1537579099096, "$lte": 1537582949212}}]
    """
    cmds = {}

    conditions = [{"DateTime": {"$gte": cmd["StartTime"], "$lte": cmd["EndTime"]}}]
    # 查询列表
    cmds["source_name"] = cmd["SourceName"]
    cmds["conditions"] = conditions

    return cmds

def parse_history_300cr_alarm(cmd):
    """
    解析查询命令
    :param cmd:
    :return:
    """
    cmds = {}
    cmds["database_name"] = "OtherData"
    cmds["source_name"] = "TestTime"

    return cmds


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


def in_high_storage(start_time, end_time):
    """
    从指定数据库中查询，给定的时间段是否在高存时间段内，返回查询结果。如果存在的话，返回所在数据库。如果不存在，返回None
    :param start_time:
    :param end_time:
    :return: database_name ：如果存在高存，返回高存的数据库；如果不存在高存，返回None
    """
    database_name = None
    client = pymongo.MongoClient()
    db = client["OtherData"]
    col = db["HighStorageLog"]

    condition = {"StartTime": {"$lte": start_time}, "EndTime":{"$gte": end_time}}

    doc = col.find_one(condition, {"_id":0})
    if doc:
        database_name = doc["DataBase"]

    client.close()  # 关闭数据库

    return database_name

def find_one_second_data(col, start_time, end_time):
    """
    从数据库OneSecondData、OneMinuteData和TenMinutesData中读取数据。供历史数据查询使用
    :param col_name: 集合名称
    :param start_time: 开始时间
    :param end_time: 结束时间
    :return:
     {"SourceName":"003ID",
      "StartTime":1537009379000, # 数据起始时间，精确到秒，然后乘以1000，到毫秒
      "Frequency":1,  # 频率。如果是秒级数据=1，如果是分钟级数据=1/60，如果是10分钟级数据=1/600
      "Data":[1,1,1,1,1],
      "DigitalLog":[]}
    """


    start_hour = int(start_time / 1000 / 3600) * 3600  # 获得整小时
    end_hour = int(end_time / 1000 / 3600) * 3600  # 获得整小时
    start_second = int(start_time/1000) % 3600  # 获得小时后的秒数
    end_second = int(end_time/1000) % 3600  # 获得小时后的秒数


    condition = {"DateTime":{"$gte": start_hour, "$lte":end_hour}}
    docs = col.find(condition).sort({"DateTime"})  # 查询数据


    data = []
    db_start_time = None  # 返回的数据中第一个数值所对应的时间，精确到秒。防止查询时间大于数据库中所有数据的时间的情况


    if start_hour == end_hour:  # 同一个小时内
        for doc in docs:
            data = doc["Data"][start_second:end_second]
            db_start_time = doc["DateTime"] + start_second
    else:  # 如果不在同一个小时内
        for doc in docs:  # 先将所有的数据连在一起，再将头和尾截掉
            data.append(doc["Data"])
            if not db_start_time:
                db_start_time = doc["DateTime"] + start_second
        end = len(data) - (3600-end_second)
        data = data[start_second:end]


    msg = dict()
    msg["SourceName"] = col.name
    msg["Data"] = data
    msg["StartTime"] = db_start_time * 1000  # 精确到毫秒
    msg["Frequency"] = 1
    msg["DigitalLog"] = []

    return msg

def find_one_minute_data(col,start_time, end_time):
    """
    从数据库OneMinuteData中读取数据。供历史数据查询使用
    :param col_name: 集合名称
    :param start_time: 开始时间
    :param end_time: 结束时间
    :return:
    {"SourceName":"003ID",
      "StartTime":1537009379000, # 数据起始时间，精确到秒，然后乘以1000，到毫秒
      "Frequency":1,  # 频率。如果是秒级数据=1，如果是分钟级数据=1/60，如果是10分钟级数据=1/600
      "Data":[1,1,1,1,1],
      "DigitalLog":[]}
    """

    start_hour = int(start_time / 1000 / 3600) * 3600  # 获得整小时
    end_hour = int(end_time / 1000 / 3600) * 3600  # 获得整小时
    start_second = int(start_time/1000) % 3600  # 获得小时后的秒数
    end_second = int(end_time/1000) % 3600  # 获得小时后的秒数
    start_minute = start_second//60  # 获得小时后的分钟数
    end_minute = end_second//60  # 获得小时后的分钟数

    condition = {"DateTime": {"$gte": start_hour, "$lte": end_hour}}
    docs = col.find(condition).sort({"DateTime"})  # 查询数据


    data = []
    db_start_time = None  # 返回的数据中第一个数值所对应的时间，精确到秒。防止查询时间大于数据库中所有数据的时间的情况

    for doc in docs:  # 先将所有的数据连在一起，再将头和尾截掉
        data.append(doc["Data"])
        if not db_start_time:
            db_start_time = doc["DateTime"] + start_minute*60  # 精确到秒
    end = len(data) - (60-end_minute)
    data = data[start_minute:end]


    msg = dict()
    msg["SourceName"] = col.name
    msg["Data"] = data
    msg["StartTime"] = db_start_time * 1000  # 精确到毫秒
    msg["Frequency"] = 1/60
    msg["DigitalLog"] = []

    return msg

def find_ten_minutes_data(col, start_time, end_time):
    """
    从数据库OneMinuteData中读取数据。供历史数据查询使用
    :param col_name: 集合名称
    :param start_time: 开始时间
    :param end_time: 结束时间
    :return:
    {"SourceName":"003ID",
      "StartTime":1537009379000, # 数据起始时间，精确到秒，然后乘以1000，到毫秒
      "Frequency":1,  # 频率。如果是秒级数据=1，如果是分钟级数据=1/60，如果是10分钟级数据=1/600
      "Data":[1,1,1,1,1],
      "DigitalLog":[]}
    """


    start_hour = int(start_time / 1000 / 3600) * 3600  # 获得整小时
    end_hour = int(end_time / 1000 / 3600) * 3600  # 获得整小时
    start_second = int(start_time/1000) % 3600  # 获得小时后的秒数
    end_second = int(end_time/1000) % 3600  # 获得小时后的秒数
    start_ten_minutes = start_second//600  # 获得小时后的十分钟数
    end_ten_minutes = end_second//600  #获得小时后的十分钟数

    condition = {"DateTime":{"$gte": start_hour, "$lte":end_hour}}
    docs = col.find(condition).sort({"DateTime"})  # 查询数据


    data = []
    db_start_time = None  # 返回的数据中第一个数值所对应的时间，精确到秒。防止查询时间大于数据库中所有数据的时间的情况

    for doc in docs:  # 先将所有的数据连在一起，再将头和尾截掉
        data.append(doc["Data"])
        if not db_start_time:
            db_start_time = doc["DateTime"] + start_ten_minutes*600  # 精确到秒
    end = len(data) - (6-end_ten_minutes)
    data = data[start_ten_minutes:end]


    msg = dict()
    msg["SourceName"] = col.name
    msg["Data"] = data
    msg["StartTime"] = db_start_time * 1000  # 精确到毫秒
    msg["Frequency"] = 1/600

    return msg

def find_alarm_data(col, start_time, end_time):
    """
    从数据库AlarmData中读取数据。供历史数据查询使用
    AlarmData中保存的是报警或数字量变化的高存数据
    :param col: 数据库的集合
    :param start_time: 开始时间
    :param end_time: 结束时间
    :return:
     {"SourceName":"003ID",
      "StartTime":1537009379000, # 数据起始时间，精确到秒，然后乘以1000，到毫秒
      "Frequency":1,  # 频率。如果是秒级数据=1，如果是分钟级数据=1/60，如果是10分钟级数据=1/600
      "Data":[1,1,1,1,1],
      "DigitalLog":[]}
    """
    msg = {}
    frequency = 10
    #  start_time和end_time精确到毫秒，需要转换为秒
    condition = {"DateTime": {"$gte": int(start_time/1000), "$lte": int(end_time/1000)}}

    docs = col.find(condition).sort("DateTime")
    data = []
    for doc in docs:
        data.append(doc["Data"])
        frequency = doc["Frequency"]



    msg["SourceName"] = col.name
    msg["Data"] = data
    msg["StartTime"] = start_time  # 精确到毫秒
    msg["Frequency"] = frequency
    msg["DigitalLog"] = find_digital_change(col.name, start_time, end_time)

    return msg

def find_test_data(col, start_time, end_time):
    """
    从数据库TestData中读取数据。供历史数据查询使用
    TestData中保存的是试验期间的高存数据
    :param col: 数据库的集合
    :param start_time: 开始时间
    :param end_time: 结束时间
    :return:
     {"SourceName":"003ID",
      "StartTime":1537009379000, # 数据起始时间，精确到秒，然后乘以1000，到毫秒
      "Frequency":1,  # 频率。如果是秒级数据=1，如果是分钟级数据=1/60，如果是10分钟级数据=1/600
      "Data":[1,1,1,1,1],
      "DigitalLog":[]}
    """
    msg = {}
    frequency = 10
    #  start_time和end_time精确到毫秒，需要转换为秒
    condition = {"DateTime": {"$gte": int(start_time/1000), "$lte": int(end_time/1000)}}

    docs = col.find(condition).sort("DateTime")
    data = []
    for doc in docs:
        data.append(doc["Data"])
        frequency = doc["Frequency"]



    msg["SourceName"] = col.name
    msg["Data"] = data
    msg["StartTime"] = start_time  # 精确到毫秒
    msg["Frequency"] = frequency
    msg["DigitalLog"] = find_digital_change(col.name, start_time, end_time)

    return msg

def find_digital_change(col_name, start_time, end_time):
    """
    从DigitalLog数据库中读取数据，以指定形式返回结果
    :param col_name: 集合名称
    :param start_time:
    :param end_time:
    :return: [{"SourceName":"700XR", "List":[{"DateTime":5, "Status":0},{"DateTime":6, "Status":1}]]

    """
    msg = []

    client = pymongo.MongoClient()
    db = client["DigitalLog"]
    # 按给定的集合名称，查找数据，如果集合存在，就查询。如果集合不存在，说明没有记录。
    if col_name in db.collection_names():
        col = db[col_name]
        condition = {"DateTime": {"$gte": int(start_time*1000), "$lte": int(end_time*1000)}}
        docs = col.find(condition, {"_id": 0}).sort("DateTime")
        tmp = []
        for doc in docs:  # 将查找到的数据放入列表
            tmp.append(doc)
        # 如果查询结果非空，将结果放入msg
        if not tmp:
            data = {}
            data["SourceName"] = col_name
            data["List"] = tmp
            # 将字典放入队列
            msg.append(data)

    client.close()  # 关闭数据库

    # 返回列表
    return msg
