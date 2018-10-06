#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:
import pymongo
import time
def find_history_alarm(q1,q2):
    # 从MongoDB查询历史报警信息HistoryAlarm数据
    client = pymongo.MongoClient()

    while True:
        try:
            cmds = q1.get()
        except:
            continue
        db = client[cmds["database_name"]]
        col = db["MainAlarm"]
        results = list()
        for condition in cmds["conditions"]:
            if cmds["topic_name"] == "History300CRAlarm":  # 如果查询300CR报警信息，保留AlarmDescription
                docs = col.find(condition, {"_id":0})
            else:  # 如果查询其他报警信息，删除AlarmDescription
                docs = col.find(condition, {"_id": 0, "AlarmDescription":0})
            for doc in docs:
                results.append(doc)
        date_time = int(time.time())
        msg = dict()  # 返回给kafka的数据
        msg["DateTime"] = date_time
        msg["ListDatas"] = list()

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

        try:
            q2.put(msg)
        except:
            continue

def find_history_digital_change(cmds):
    """
    从MongoDB查询开关量变化信息history digital change数据，返回kafka预定义的topic
    :param cmds:
            cmds = {}
            conditions = [{"DateTime": {"$gte": 1537579099096, "$lte": 1537582949212}}]
            cmds["topic_name"] = "HistoryDigitalChangeCmd"
            cmds["database_name"] = "DigitalLog"
            cmds["source_name"] = "531XR"
            cmds["conditions"] = conditions
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
    msg = {}  # 返回给kafka的数据
    msg["DateTime"] = int(time.time())
    msg["ListDatas"] = list()
    # 初始化数据库连接
    client = pymongo.MongoClient()
    db = client[cmds["database_name"]]

    if cmds["source_name"] == "All":  # 按时间查询
        for col_name in db.collection_names():  # 获取数据库下所有的集合
            if col_name != "system.indexes":
                result = {}
                result["List"] = []
                col = db[col_name]
                for condition in cmds["conditions"]:
                    docs = col.find(condition, {"_id": 0})  # cmds["conditions"]只有一个元素
                    for doc in docs:
                        result["List"].append(doc)
                result["SourceName"] = col_name
            msg["ListDatas"].append(result)
    else:  # 按测点查询
        col = db[cmds["source_name"]]  # 连接指定集合
        result = {}
        result["List"] = []
        result["SourceName"] = cmds["source_name"]
        for condition in cmds["conditions"]:
            docs = col.find(condition, {"_id": 0}) # cmds["conditions"]只有一个元素
            for doc in docs:
                result["List"].append(doc)
        msg["ListDatas"].append(result)

    return msg

def find_history_data(cmds):
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
    msg = {}  # 返回给kafka的数据
    msg["DateTime"] = int(time.time())
    msg["ListDatas"] = list()
    # 初始化数据库连接
    client = pymongo.MongoClient()
    db = client[cmds["database_name"]]

    for source_name in cmds["source_name"]:
        col = db[source_name]
        result = {}
        result["List"] = []
        result["SourceName"] = cmds["source_name"]
        for condition in cmds["conditions"]:
            docs = col.find(condition, {"_id": 0}).sort("")
            for doc in docs:
                result["List"].append(doc)

    if cmds["source_name"] == "All":  # 按时间查询
        for col_name in db.collection_names():  # 获取数据库下所有的集合
            if col_name != "system.indexes":
                result = {}
                result["List"] = []
                col = db[col_name]
                for condition in cmds["conditions"]:
                    docs = col.find(condition, {"_id": 0})  # cmds["conditions"]只有一个元素
                    for doc in docs:
                        result["List"].append(doc)
                result["SourceName"] = col_name
            msg["ListDatas"].append(result)
    else:  # 按测点查询
        col = db[cmds["source_name"]]  # 连接指定集合
        result = {}
        result["List"] = []
        result["SourceName"] = cmds["source_name"]
        for condition in cmds["conditions"]:
            docs = col.find(condition, {"_id": 0}) # cmds["conditions"]只有一个元素
            for doc in docs:
                result["List"].append(doc)
        msg["ListDatas"].append(result)

    return msg

def find_test_info(cmds):
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

    client = pymongo.MongoClient()
    db = client[cmds["database_name"]]
    col = db["source_name"]

    docs = col.find({},{"_id":0}).sort("TestTime")
    msg = {}
    msg["ListDatas"] = []
    for doc in docs:
        msg["ListDatas"].append(doc)

    msg["DateTime"] = int(time.time()) * 1000
    return msg



