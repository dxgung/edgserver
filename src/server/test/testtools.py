#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:
import pymongo
import random
import datetime
import time

def get_timestamp():
    # 返回以小时为精度的时间戳

    # 获取当前时间
    dt = datetime.datetime.now().strftime("%Y-%m-%d %H")
    # 转换为时间数组
    timeArray = time.strptime(dt, "%Y-%m-%d %H")
    # 按小时转换为时间戳
    timestamp = int(time.mktime(timeArray))
    return timestamp

def create_db(database_name):

    # 仿真数据的起始时间是2018/9/23 12:00:00，对应时间戳是1537675200000
    # 自动添加数据库
    client = pymongo.MongoClient()
    # 由于MongoDB是惰性的，在插入第一条文档之前，数据库和collection都不会真的生成
    if database_name == "OneSecondData":
        db = client['OneSecondData']  # 创建数据库
        col = db['300MT']  # 创建collection
        DateTime = get_timestamp()
        Data = 0
        doc = {"DateTime": 0, "Data": [0]}
        col.insert_one(doc)
    if database_name == "OneMinuteData":
        pass
    if database_name == "TenMinutesData":
        pass
    if database_name == "TestData":
        pass
    if database_name == "DigitalLog":
        db = client[database_name]
        col = db["531XR"]  # 创建collection
        docs = list()

        for i in range(10):
            d = dict()
            d["DateTime"] = round(time.time(), 3)*1000  # 精确到毫秒
            d["Status"] = random.randint(0, 1)
            docs.append(d)
            time.sleep(1)
        # 写入数据库
        col.insert_many(docs)

    if database_name == "AlarmLog":  # 创建AlarmLog数据库
        db = client[database_name]  # 创建数据库
        col = db['MainAlarm']  # 创建collection
        docs = list()
        d = dict()
        d["AlarmSource"] = "301MT"
        d["StartTime"] = 1537495201  # 报警开始时间
        d["EndTime"] = 1537495240  # 报警结束时间
        d["AlarmValue"] = 100  # 报警值，报警触发时，报警源的值
        d["ThresholdValue"] = 50  # 报警阈值
        d["AlarmDescription"] = "超温报警"  #
        col.insert_one(d)


    if database_name == "AlarmData":
        db = client[database_name]  # 创建数据库
        col = db['300MT']  # 创建collection
        docs = list()
        for i in range(40):
            d = dict()
            d["AlarmTime"] = 1537495210  # 2018-09-21 10:00:10
            d["StartTime"] = 1537495201  # 高存持续时间从报警开始前9秒，到报警结束后30秒，一共是40秒
            d["EndTime"] = 1537495240
            d["DateTime"] = 1537495201 + i
            data_list = list()
            for j in range(10):
                data_list.append(random.random())
            d["Data"] = data_list
            docs.append(d)
        col.insert_many(docs)
    # if database_name == "OtherData":
        # d = {}
        # start_time =    #  2018/9/23 12:00:00
        # d["TestTime"] =
    print("ok")
def create_db2():
    # 向数据库中插入仿真的集合和文档
    client = pymongo.MongoClient()
    db = client["OneSecondData"]
    for i in range(100):
        d = dict()
        col = str(i).rjust(3,"0") + "MT"
        d["DateTime"] = int(time.time()/3600)*3600
        d["Data"] = [0, 1, 2]
        db[col].insert_one(d)
    print("Done")

