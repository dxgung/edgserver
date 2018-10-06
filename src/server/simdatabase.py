#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:
import pymongo
import datetime
import time
import random
def create_db(database_name):

    # 仿真数据的起始时间是2018/9/23 12:00:00，对应时间戳是1537675200000
    # 自动添加数据库
    client = pymongo.MongoClient()
    # 由于MongoDB是惰性的，在插入第一条文档之前，数据库和collection都不会真的生成
    if database_name == "OneSecondData":
        db = client[database_name]  # 创建数据库
        col = db['300MT']  # 创建collection
        docs = []
        start_time = 1537675200000
        for i in range(10):
            doc = {}
            l = list()
            doc["DateTime"] = int(start_time/3600000 + i*3600000)
            for j in range(3600):
                l.append(random.random())
            doc["Data"] = l
            docs.append(doc)
        col.insert_many(docs)

    if database_name == "OneMinuteData":
        db = client[database_name]  # 创建数据库
        col = db['300MT']  # 创建collection
        docs = []
        start_time = 1537675200000
        for i in range(10):
            doc = {}
            l = list()
            doc["DateTime"] = int(start_time / 3600000 + i * 3600000)
            for j in range(60):
                l.append(random.random())
            doc["Data"] = l
            docs.append(doc)
        col.insert_many(docs)

    if database_name == "TenMinutesData":
        db = client[database_name]  # 创建数据库
        col = db['300MT']  # 创建collection
        docs = []
        start_time = 1537675200000
        for i in range(10):
            doc = {}
            l = list()
            doc["DateTime"] = int(start_time / 3600000 + i * 3600000)
            for j in range(6):
                l.append(random.random())
            doc["Data"] = l
            docs.append(doc)
        col.insert_many(docs)


    if database_name == "TestData":
        db = client[database_name]  # 创建数据库
        col = db['300MT']  # 创建collection
        docs = []
        start_time = 1537675200000
        for i in range(10):
            doc = {}
            l = list()
            doc["TestTime"] = int(start_time / 1000)
            doc["DateTime"] = int(start_time / 1000 + i)
            for j in range(10):
                l.append(random.random())
            doc["Data"] = l
            docs.append(doc)
        col.insert_many(docs)


    if database_name == "DigitalLog":
        db = client[database_name]
        col = db["300XR"]  # 创建collection
        docs = list()
        start_time = 1537675200000
        for i in range(10):
            d = dict()
            d["DateTime"] = start_time + i*10000  # 精确到毫秒  10秒产生一个文档
            d["Status"] = random.randint(0, 1)
            docs.append(d)

        # 写入数据库
        col.insert_many(docs)

    if database_name == "AlarmLog":  # 创建AlarmLog数据库
        db = client[database_name]  # 创建数据库
        col = db['MainAlarm']  # 创建collection
        docs = list()
        d = dict()
        start_time = 1537675200000
        for i in range(10):
            d = {}
            d["AlarmSource"] = "300MT"
            d["StartTime"] = 1537675200000 + i*30000  # 报警开始时间
            d["EndTime"] = 1537675200000 + i*30000 + 20000  # 报警结束时间 报警持续了20秒
            d["AlarmValue"] = 100  # 报警值，报警触发时，报警源的值
            d["ThresholdValue"] = 50  # 报警阈值
            d["AlarmDescription"] = "超温报警"  #
            docs.append(d)
        col.insert_many(docs)


    if database_name == "AlarmData":
        db = client[database_name]  # 创建数据库
        col = db['300MT']  # 创建collection
        docs = list()
        start_time = 1537675200000
        for i in range(40):
            d = dict()
            d["AlarmTime"] = start_time + i*100000  #
            d["StartTime"] = start_time + i*100000 - 9000 # 高存持续时间从报警开始前9秒，到报警结束后30秒，一共是40秒
            d["EndTime"] = start_time + i*100000 + 30000
            d["DateTime"] = start_time + i*1000 - 9000  # 数据记录时间
            data_list = list()
            for j in range(10):
                data_list.append(random.random())
            d["Data"] = data_list
            docs.append(d)
        col.insert_many(docs)

    if database_name == "OtherData":
        db = client[database_name]  # 创建数据库
        col = db['TestTime']  # 创建collection
        docs = []
        start_time = 1537675200000   #  2018/9/23 12:00:00
        for i in range(10):
            doc = {}
            doc["TestTime"] = start_time + i*100000
            doc["StartupTime"] = start_time + i * 100000 - 10000  # 启动前10秒
            doc["StopTime"] = start_time + i * 100000 + 30000  # 启动后30秒
            docs.append(doc)
        col.insert_many(docs)
        col = db["HighStorageLog"]
        docs = []
        for i in range(10):
            doc = {}
            doc["DataBase"] = "TestData"
            doc["StartTime"] = start_time + i * 100000 - 10000  # 启动前10秒
            doc["StopTime"] = start_time + i * 100000 + 30000  # 启动后30秒
            docs.append(doc)
        col.insert_many(docs)

    client.close()
    print("ok")

def get_timestamp():
    # 返回以小时为精度的时间戳

    # 获取当前时间
    dt = datetime.datetime.now().strftime("%Y-%m-%d %H")
    # 转换为时间数组
    timeArray = time.strptime(dt, "%Y-%m-%d %H")
    # 按小时转换为时间戳
    timestamp = int(time.mktime(timeArray))
    return timestamp


if __name__ == '__main__':

    DBs = ["OneSecondData",
           "OneMinuteData",
           "TenMinutesData",
           "TestData",
           "DigitalLog",
           "AlarmData",
           "AlarmLog",
           "OtherData"]
    create_db(DBs[5])