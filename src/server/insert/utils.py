#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:
import pymongo
import datetime
import time
import random
import pandas as pd
import redis
import threading
from confluent_kafka import Consumer,Producer,KafkaError,KafkaException
import json
import queue
import sys
sys.path.append("/root/root/usr/open/edgserver/src")
if True:
    from settings import BROKER, TEMPERATURE, ANALOG, DIGITAL

QUITE = False
EXPIRE_TIME = 60  # 过期时间，精确到秒



def get_timestamp():
    # 返回以小时为精度的时间戳

    # 获取当前时间
    dt = datetime.datetime.now().strftime("%Y-%m-%d %H")
    # 转换为时间数组
    timeArray = time.strptime(dt, "%Y-%m-%d %H")
    # 按小时转换为时间戳
    timestamp = int(time.mktime(timeArray))
    return timestamp

def simulate_data(q):
    # 模拟从kafka获取的数据
    while True:
        msg = dict()
        data = dict()
        msg["datetime"] = int(time.time())
        for i in range(100):
            name = str(i).rjust(3, "0") + "MT"
            data[name] = random.random()
        msg["datas"] = data
        try:
            q.put(msg, block=False)
            # print(msg)
        except:
            continue
        time.sleep(1)

def fill():
    # 数据库补齐
    # 主要补齐OneSecondData, OneMinuteData和
    client = pymongo.MongoClient()
    dbs = [client["OneSecondData"], client["OneMinuteData"], client["TenMinutesData"]]


    one_second_zeros = [0] * 3600  # 全零列表，用于补齐
    one_minute_zeros = [0] * 60  # 全零列表，用于补齐
    ten_minutes_zeros = [0] * 6  # 全零列表，用于补齐
    zeros = [one_second_zeros, one_minute_zeros, ten_minutes_zeros]

    is_fill = False
    index = 0

    current_timestamp = get_timestamp()  # 当前时间戳，精确到小时

    for db in dbs:
        # 先判断是否需要补齐
        for col in db.collection_names():
            if col != "system.indexes":
                # 找到最新的一条记录
                res = db[col].find({}, {"_id": 0}).sort([{"DateTime", -1}]).limit(1)
                # 如果最新的一条记录小于当前时间，说明需要补齐
                if res[0]["DateTime"] < current_timestamp:
                    is_fill = True
                    db_current_timestamp = res[0]["DateTime"]  # 获取数据库当前时间戳
                    break
        #  补齐操作  所有空缺都补零
        if is_fill:

            for col in db.collection_names():
                if col != "system.indexes":
                    fill_list = list()  # 用于补齐的列表
                    for i in range(int((current_timestamp - db_current_timestamp) / 3600) + 1):
                        tmp = dict()
                        tmp["DateTime"] = db_current_timestamp + 3600 * (i + 1)
                        tmp["Data"] = zeros[index]
                        fill_list.append(tmp)
                        # print(len(fill_list))
                    db[col].insert_many(fill_list)
            is_fill = False
            index += 1



def init_documents(pool):
    # 从数据库中OneSecondData读取最新的文档
    """
    docs
    :return: docs
    docs = [{"300MT":{"DateTime":1, "Datas":[1,2,3]},{"301MT":{"DateTime":1, "Datas":[1,2,3]}}}] # 秒级数据

    """

    client = pymongo.MongoClient()
    r0 = redis.Redis(connection_pool=pool)

    db = client["OneSecondData"]
    # 从数据库读取最新的文档
    db_docs = dict()
    for key in TEMPERATURE:
        res = db[TEMPERATURE[key]].find({}, {"_id":0}).sort([{"DateTime", -1}]).limit(1)  # 查找最新的文档
        db_docs[TEMPERATURE[key]] = res[0]
    # 将数据库中的最新文档放在redis中
    r0.set("Init:temperature", db_docs)

    db_docs = dict()
    for key in ANALOG:
        res = db[ANALOG[key]].find({}, {"_id":0}).sort([{"DateTime", -1}]).limit(1)  # 查找最新的文档
        db_docs[ANALOG[key]] = res[0]

    # 将数据库中的最新文档放在redis中
    r0.set("Init:analog", db_docs)

    db_docs = dict()
    for key in DIGITAL:
        res = db[DIGITAL[key]].find({}, {"_id": 0}).sort([{"DateTime", -1}]).limit(1)  # 查找最新的文档
        db_docs[DIGITAL[key]] = res[0]

    # 将数据库中的最新文档放在redis中
    r0.set("Init:digital", db_docs)




def simulate_history_alarm_cmd():
    # 模拟发出历史报警信息查询
    # cmd = dict()
    # cmd["SourceName"] = "300MT"
    # cmd["StartTime"] =  # 时间戳，精确到秒
    # cmd["EndTime"] =
    pass

def in_high_storage(start_time, end_time):
    """
    从指定数据库中查询，给定的时间段是否在高存时间段内，返回查询结果。如果在的话，返回所在数据库
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

    return database_name

def find_one_second_data(db_name, col_name, start_time, end_time):
    """
    从数据库OneSecondData、OneMinuteData和TenMinutesData中读取数据。供历史数据查询使用
    :param db_name: 数据库名称
    :param col_name: 集合名称
    :param start_time: 开始时间
    :param end_time: 结束时间
    :return:
    """
    client = pymongo.MongoClient()
    db = client[db_name]
    col = db[col_name]

    start_hour = int(start_time / 1000 / 3600) * 3600  # 获得整小时
    end_hour = int(end_time / 1000 / 3600) * 3600  # 获得整小时
    start_second = int(start_time/1000) % 3600  # 获得小时后的秒数
    end_second = int(end_time/1000) % 3600  # 获得小时后的秒数
    start_minute = start_second//60  # 获得小时后的分钟数
    end_minute = end_second//60  # 获得小时后的分钟数
    start_ten_minutes = start_minute//10  # 获得小时后的十分钟数
    end_ten_minutes = end_second//10  #获得小时后的十分钟数

    condition = {"DateTime":{"$gte": start_hour, "$lte":end_hour}}
    docs = col.find(condition, {}).sort({"DateTime":1})  # 查询数据
    client.close()  # 关闭服务器连接

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
    msg["SourceName"] = col_name
    msg["Data"] = data
    msg["StartTime"] = db_start_time * 1000  # 精确到毫秒
    msg["Frequency"] = 1

    return msg

def find_one_minute_data(col_name, start_time, end_time):
    """
    从数据库OneMinuteData中读取数据。供历史数据查询使用
    :param col_name: 集合名称
    :param start_time: 开始时间
    :param end_time: 结束时间
    :return:
    """
    client = pymongo.MongoClient()
    db = client["OneMinuteData"]
    col = db[col_name]

    start_hour = int(start_time / 1000 / 3600) * 3600  # 获得整小时
    end_hour = int(end_time / 1000 / 3600) * 3600  # 获得整小时
    start_second = int(start_time/1000) % 3600  # 获得小时后的秒数
    end_second = int(end_time/1000) % 3600  # 获得小时后的秒数
    start_minute = start_second//60  # 获得小时后的分钟数
    end_minute = end_second//60  # 获得小时后的分钟数

    condition = {"DateTime":{"$gte": start_hour, "$lte":end_hour}}
    docs = col.find(condition, {}).sort({"DateTime":1})  # 查询数据
    client.close()  # 关闭服务器连接

    data = []
    db_start_time = None  # 返回的数据中第一个数值所对应的时间，精确到秒。防止查询时间大于数据库中所有数据的时间的情况

    for doc in docs:  # 先将所有的数据连在一起，再将头和尾截掉
        data.append(doc["Data"])
        if not db_start_time:
            db_start_time = doc["DateTime"] + start_minute*60  # 精确到秒
    end = len(data) - (60-end_minute)
    data = data[start_minute:end]


    msg = dict()
    msg["SourceName"] = col_name
    msg["Data"] = data
    msg["StartTime"] = db_start_time * 1000  # 精确到毫秒
    msg["Frequency"] = 1/60

    return msg

def find_ten_minutes_data(col_name, start_time, end_time):
    """
    从数据库OneMinuteData中读取数据。供历史数据查询使用
    :param col_name: 集合名称
    :param start_time: 开始时间
    :param end_time: 结束时间
    :return:
    """
    client = pymongo.MongoClient()
    db = client["TenMinutesData"]
    col = db[col_name]

    start_hour = int(start_time / 1000 / 3600) * 3600  # 获得整小时
    end_hour = int(end_time / 1000 / 3600) * 3600  # 获得整小时
    start_second = int(start_time/1000) % 3600  # 获得小时后的秒数
    end_second = int(end_time/1000) % 3600  # 获得小时后的秒数
    start_ten_minutes = start_second//600  # 获得小时后的十分钟数
    end_ten_minutes = end_second//600  #获得小时后的十分钟数

    condition = {"DateTime":{"$gte": start_hour, "$lte":end_hour}}
    docs = col.find(condition, {}).sort({"DateTime":1})  # 查询数据
    client.close()  # 关闭服务器连接

    data = []
    db_start_time = None  # 返回的数据中第一个数值所对应的时间，精确到秒。防止查询时间大于数据库中所有数据的时间的情况

    for doc in docs:  # 先将所有的数据连在一起，再将头和尾截掉
        data.append(doc["Data"])
        if not db_start_time:
            db_start_time = doc["DateTime"] + start_ten_minutes*600  # 精确到秒
    end = len(data) - (6-end_ten_minutes)
    data = data[start_ten_minutes:end]


    msg = dict()
    msg["SourceName"] = col_name
    msg["Data"] = data
    msg["StartTime"] = db_start_time * 1000  # 精确到毫秒
    msg["Frequency"] = 1/600

    return msg


def find_digital_change(col_names, start_time, end_time):
    """
    从DigitalLog数据库中读取数据，以指定形式返回结果
    :param col_name: 集合名称
    :param start_time:
    :param end_time:
    :return: {"510XR":[{"DateTime":5, "Status":0},{"DateTime":6, "Status":1}],
              "511XR":[{"DateTime":5, "Status":0},{"DateTime":6, "Status":1}]}
    """
    msg = {}

    client = pymongo.MongoClient()
    db = client["DigitalLog"]
    # 按给定的集合名称，查找数据
    for col_name in col_names:
        col = db[col_name]
        condition = {"DateTime":{"$gte": start_time, "$lte": end_time}}
        docs = col.find(condition, {"_id":0}).sort("DateTime")
        tmp = []
        for doc in docs:  # 将查找到的数据放入列表
            tmp.append(doc)
        msg[col_name] = tmp  # 将列表存入字典
    client.close()
    # 返回字典
    return msg

def temperature_data_analysis(pool0, pool1):
    """
    从redis中取出温度信号，对其进行处理后，送入队列
    :param pool:

    :return:
    """
    d_time = None
    one_second_zeros = [0] * 3600  # 全零列表，用于补齐
    tmp_one_minute_data = [0] * 60  # 全零列表，用于补齐
    tmp_ten_minutes_data = [0] * 6  # 全零列表，用于补齐

    # one_second_value = {"300MT":[1,2,...,2], "301MT":[1,2,...,2]}
    one_second_value = {}  # 字典，用于保存秒级数据




    one_second_datas = dict()

    r0 = redis.StrictRedis(connection_pool=pool0)
    r1 = redis.StrictRedis(connection_pool=pool1)

    data_from_db = r0.get("Init:temperature")
    data_from_db = eval(data_from_db)


    # t_datas = {"DateTime": 1, "Datas": {"300MT": 1, "301MT": 1}}
    while not QUITE:

        try:
            t_datas = r1.brpop("OneSecond:temperature")
            t_datas = eval(t_datas[1])
        except Exception as e:
            print(e)
            continue
        # 时间初始化

        if not d_time:
            d_time = int(t_datas['DateTime'] / 1000 / 3600) * 3600  # 将时间戳转换为整小时
            # 根据数据库中的最新的数据，初始化datas; key来自redis中的数据
        for key in t_datas["Datas"]:
            one_second_datas[key] = {}
            one_second_datas[key]["doc"] = {}
            one_second_datas[key]["Datas"] = data_from_db[key]["Datas"]












        # 如果到下一个小时，更新d_time, 初始化datas
        if t_datas['DateTime']/1000 - d_time > 3600:
            d_time = int(t_datas['DateTime'] / 1000 / 3600) * 3600  # 将时间戳转换为整小时
            one_second_datas = dict()


        counter = t_datas['DateTime'] - d_time  # 当前小时的秒数。当前时间戳减去当前整小时的时间戳。

        #
        if not one_second_datas:
            #  如果是整小时的第一条记录，修改DateTime的值
            for key in t_datas["Datas"]:
                one_second_datas[key] = dict()
                one_second_datas[key]['DateTime'] = d_time
                one_second_datas[key]['Datas'] = one_second_zeros  # 预填充为全零列表
                one_second_datas[key]['Datas'][counter] = t_datas["Datas"][key]
        else:
            # 如果不是整小时的第一条记录，msg赋值
            for key in t_datas["Datas"]:
                one_second_datas[key]['Datas'][counter] = t_datas["Datas"][key]
                datas_key = "Datas"+"."+str(counter)
                datas_value = t_datas["Datas"][key]

                one_second_datas[key]["docs"].update({datas_key: datas_value})


        # 5秒钟更新一次 如果counter是零，应该更新
        one_second_datas = dict()
        if not counter % 5:
            start = (counter // 5) * 5
            end = (start + 1) * 5
            for key in t_datas["Datas"]:

            try:
                r0.lpush("DB:OneSecondData", one_second_datas)
                break
            except:
                continue

        # 1分钟更新一次
        one_minute_datas = dict()
        if not counter % 60:
            start = (counter // 60) * 60
            end = (start + 1) * 60
            for key in t_datas["Datas"]:
                s = one_second_value[key]
                mean = list_mean(s[start:end])  # 从秒级数据中取60个数据，求平均
                # doc = {"Datas.23":43}
                # 这样设计的目的是为了以更新MongoDB中数组元素的方式，将数值插入到数组Datas中。
                doc = {"Datas"+"."+str(counter // 60): mean}
                one_minute_datas[key] = {"DateTime": d_time, "Datas": doc}
            try:
                r0.lpush("DB:OneMinuteData", one_minute_datas)
            except:
                continue

        # 10分钟更新一次
        ten_minutes_datas = dict()
        if not counter % 600:
            start = (counter // 600) * 600
            end = (start + 1) * 600
            for key in t_datas["datas"]:
                s = one_second_value[key]
                mean = list_mean(s[start:end]) # 从秒级数据中取600个数据，求平均
                doc = {"Datas"+"."+str(counter // 600): mean}
                ten_minutes_datas[key] = {"DateTime": d_time, "Datas": doc}
            try:
                r0.lpush("DB:TenMinutesData", ten_minutes_datas)
            except:
                continue


def analog_data_analysis(pool):
    """
    从redis中取出温度信号，对其进行处理后，送入redis
    :param pool:
:
    :return:
    """
    d_time = None
    one_second_zeros = [0] * 3600  # 全零列表，用于补齐
    one_minute_zeros = [0] * 60  # 全零列表，用于补齐
    ten_minutes_zeros = [0] * 6  # 全零列表，用于补齐

    one_second_datas = dict()

    r0 = redis.Redis(connection_pool=pool)  # 模拟量数据

    # current_temperature_data的格式
    # current_temperature_data = {"DateTime": 1, "Datas": {"300MT": 1, "301MT": 1}}
    while not QUITE:

        try:
            t_datas = r0.brpop("OneSecond:Analog")
            data_from_db = r0.brpop("Init:Docs")
            t_datas = eval(t_datas[1])
            data_from_db = eval(data_from_db[1])
        except:
            continue
        # 时间初始化
        if not d_time:
            d_time = int(t_datas['DateTime'] / 1000 / 3600) * 3600  # 将时间戳转换为整小时

            # 根据数据库中的最新的数据，初始化datas; key来自redis中的数据
            for key in t_datas["Datas"]:
                one_second_datas[key] = {}
                one_second_datas[key]["Datas"] = data_from_db[key]

        # 如果到下一个小时，更新d_time, 初始化datas
        if t_datas['DateTime'] / 1000 - d_time > 3600:
            d_time = int(t_datas['DateTime'] / 1000 / 3600) * 3600  # 将时间戳转换为整小时
            one_second_datas = dict()

        counter = t_datas['DateTime'] - d_time  # 当前小时的秒数。当前时间戳减去当前整小时的时间戳。

        #
        if not one_second_datas:
            #  如果是整小时的第一条记录，修改DateTime的值
            for key in t_datas["Datas"]:
                one_second_datas[key] = dict()
                one_second_datas[key]['DateTime'] = d_time
                one_second_datas[key]['Datas'] = one_second_zeros  # 预填充为全零列表
                one_second_datas[key]['Datas'][counter] = t_datas["Datas"][key]
        else:
            # 如果不是整小时的第一条记录，msg赋值
            for key in t_datas["Datas"]:
                one_second_datas[key]['Datas'][counter] = t_datas["Datas"][key]

        # 5秒钟更新一次 如果counter是零，应该更新
        if not counter % 5:
            try:
                r0.lpush("DB:OneSecondData", one_second_datas)
            except:
                continue

        # 1分钟更新一次
        one_minute_datas = dict()
        if not counter % 60:

            for key in t_datas["Datas"]:
                means = list()
                s = one_second_datas[key]['Datas']
                for i in range(60):
                    means.append(list_mean(s[i * 60:(i + 1) * 60]))

                one_minute_datas[key] = {"DateTime": d_time, "Datas": means}
            try:
                r0.lpush("DB:OneMinuteData", one_minute_datas)
            except:
                continue

        # 10分钟更新一次
        ten_minutes_datas = dict()
        if not counter % 600:
            for key in t_datas["datas"]:
                means = list()
                s = one_minute_datas[key]['Datas']
                for i in range(6):
                    means.append(list_mean(s[i * 10:(i + 1) * 10]))
                ten_minutes_datas[key] = {"DateTime": d_time, "Datas": means}
            try:
                r0.lpush("DB:TenMinutesData", ten_minutes_datas)
            except:
                continue
def digital_data_analysis(pool):
    """
    从redis中取出温度信号，对其进行处理后，送入队列
    :param pool:
    :param pool:
    :return:
    """
    d_time = None
    one_second_zeros = [0] * 3600  # 全零列表，用于补齐

    one_second_datas = dict()

    r0 = redis.Redis(connection_pool=pool)  # 开关量数据

    # current_temperature_data的格式
    # current_temperature_data = {"DateTime": 1, "Datas": {"300MT": 1, "301MT": 1}}
    while True:

        try:
            t_datas = r0.brpop("OneSecond:Digital")
            data_from_db = r0.brpop("Init:Docs")
            t_datas = eval(t_datas[1])
            data_from_db = eval(data_from_db[1])
        except:
            continue
        # 时间初始化
        if not d_time:
            d_time = int(t_datas['DateTime'] / 1000 / 3600) * 3600  # 将时间戳转换为整小时

            # 根据数据库中的最新的数据，初始化datas; key来自redis中的数据
            for key in t_datas["Datas"]:
                one_second_datas[key] = {}
                one_second_datas[key]["Datas"] = data_from_db[key]

        # 如果到下一个小时，更新d_time, 初始化datas
        if t_datas['DateTime'] / 1000 - d_time > 3600:
            d_time = int(t_datas['DateTime'] / 1000 / 3600) * 3600  # 将时间戳转换为整小时
            one_second_datas = dict()

        counter = t_datas['DateTime'] - d_time  # 当前小时的秒数。当前时间戳减去当前整小时的时间戳。

        #
        if not one_second_datas:
            #  如果是整小时的第一条记录，修改DateTime的值
            for key in t_datas["Datas"]:
                one_second_datas[key] = dict()
                one_second_datas[key]['DateTime'] = d_time
                one_second_datas[key]['Datas'] = one_second_zeros  # 预填充为全零列表
                one_second_datas[key]['Datas'][counter] = t_datas["Datas"][key]
        else:
            # 如果不是整小时的第一条记录，msg赋值
            for key in t_datas["Datas"]:
                one_second_datas[key]['Datas'][counter] = t_datas["Datas"][key]

        # 5秒钟更新一次 如果counter是零，应该更新
        if not counter % 5:
            try:
                r0.lpush("DB:OneSecondData", one_second_datas)
            except:
                continue

        # 1分钟更新一次
        one_minute_datas = dict()
        if not counter % 60:

            for key in t_datas["Datas"]:
                means = list()
                s = one_second_datas[key]['Datas']
                for i in range(60):
                    means.append(list_mean(s[i * 60:(i + 1) * 60]))

                one_minute_datas[key] = {"DateTime": d_time, "Datas": means}
            try:
                r0.lpush("DB:OneMinuteData", one_minute_datas)
            except:
                continue

        # 10分钟更新一次
        ten_minutes_datas = dict()
        if not counter % 600:
            for key in t_datas["datas"]:
                means = list()
                s = one_minute_datas[key]['Datas']
                for i in range(6):
                    means.append(list_mean(s[i * 10:(i + 1) * 10]))
                ten_minutes_datas[key] = {"DateTime": d_time, "Datas": means}
            try:
                r0.lpush("DB:TenMinutesData", ten_minutes_datas)
            except:
                continue

def dynamic_data_analysis(pool):
    """
    从redis中取出温度信号，对其进行处理后，送入队列
    :param q1:

    :return:
    """
    d_time = None
    one_second_zeros = [0] * 3600  # 全零列表，用于补齐
    one_minute_zeros = [0] * 60  # 全零列表，用于补齐
    ten_minutes_zeros = [0] * 6  # 全零列表，用于补齐

    one_second_datas = dict()


    r0 = redis.Redis(connection_pool=pool)  # 开关量数据


    # current_temperature_data的格式
    # current_temperature_data = {"DateTime": 1, "Datas": {"300MT": 1, "301MT": 1}}
    while not QUITE:

        try:
            t_datas = r0.brpop("OneSecond:Dynamic")
            data_from_db = r0.brpop("Init:Docs")
            t_datas = eval(t_datas[1])
            data_from_db = eval(data_from_db[1])
        except:
            continue
        # 时间初始化
        if not d_time:
            d_time = int(t_datas['DateTime'] / 1000 / 3600) * 3600  # 将时间戳转换为整小时

            # 根据数据库中的最新的数据，初始化datas; key来自redis中的数据
            for key in t_datas["Datas"]:
                one_second_datas[key] = {}
                one_second_datas[key]["Datas"] = data_from_db[key]

        # 如果到下一个小时，更新d_time, 初始化datas
        if t_datas['DateTime'] / 1000 - d_time > 3600:
            d_time = int(t_datas['DateTime'] / 1000 / 3600) * 3600  # 将时间戳转换为整小时
            one_second_datas = dict()

        counter = t_datas['DateTime'] - d_time  # 当前小时的秒数。当前时间戳减去当前整小时的时间戳。

        #
        if not one_second_datas:
            #  如果是整小时的第一条记录，修改DateTime的值
            for key in t_datas["Datas"]:
                one_second_datas[key] = dict()
                one_second_datas[key]['DateTime'] = d_time
                one_second_datas[key]['Datas'] = one_second_zeros  # 预填充为全零列表
                one_second_datas[key]['Datas'][counter] = t_datas["Datas"][key]
        else:
            # 如果不是整小时的第一条记录，msg赋值
            for key in t_datas["Datas"]:
                one_second_datas[key]['Datas'][counter] = t_datas["Datas"][key]

        # 5秒钟更新一次 如果counter是零，应该更新
        if not counter % 5:
            try:
                r0.lpush("DB:OneSecondData", one_second_datas)
            except:
                continue

        # 1分钟更新一次
        one_minute_datas = dict()
        if not counter % 60:

            for key in t_datas["Datas"]:
                means = list()
                s = one_second_datas[key]['Datas']
                for i in range(60):
                    means.append(list_mean(s[i * 60:(i + 1) * 60]))

                one_minute_datas[key] = {"DateTime": d_time, "Datas": means}
            try:
                r0.lpush("DB:OneMinuteData", one_minute_datas)
            except:
                continue

        # 10分钟更新一次
        ten_minutes_datas = dict()
        if not counter % 600:
            for key in t_datas["datas"]:
                means = list()
                s = one_minute_datas[key]['Datas']
                for i in range(6):
                    means.append(list_mean(s[i * 10:(i + 1) * 10]))
                ten_minutes_datas[key] = {"DateTime": d_time, "Datas": means}
            try:
                r0.lpush("DB:TenMinutesData", ten_minutes_datas)
            except:
                continue

def low_insert_to_db(pool, client):
    """
    将秒级数据、写入MongoDB中。
    :param queues = [q1, q2, q3]: q1：OneSecondData；q2：OneMinuteData；q3：TenMinutesData
    :return:
    """
    data = dict()
    #
    one_second_data = client["OneSecondData"]
    one_minute_data = client["OneMinuteData"]
    ten_minutes_data = client["TenMinutesData"]

    r0 = redis.Redis(connection_pool=pool)

    while not QUITE:

        # 从队列中获取1秒数据
        try:
            one_second_datas = r0.brpop("DB:OneSecondData")
            if one_second_datas:
                one_second_datas = eval(one_second_datas[1])
        except:
            continue
        # datas = {"300MT":{"DataTime":123, "Data":[1,2,3,4]}}
        # 将数据插入到1秒数据库中
        for key in one_second_datas:
            one_second_data[key].find_one_and_update({"DateTime":one_second_datas[key]["DateTime"]},
                                                     {"$set":{"Data":one_second_datas[key]["Data"]}})

        # 从队列中获取1分钟数据
        try:
            one_minute_datas = r0.brpop("DB:OneMinuteData")
            if one_minute_datas:
                one_minute_datas = eval(one_minute_datas[1])
        except:
            continue
        # 将数据插入到1分钟数据库中
        for key in one_minute_datas:
            one_minute_data[key].find_one_and_update({"DateTime":one_minute_datas[key]["DateTime"]},
                                                     {"$set":{"Data":one_minute_datas[key]["Data"]}})

        # 从队列获取10分钟数据
        try:
            ten_minutes_datas = r0.brpop("DB:TenMinutesData")
            if ten_minutes_datas:
                ten_minutes_datas = eval(one_minute_datas[1])
        except:
            continue
        # 将数据插入到10分钟数据库中
        for key in ten_minutes_datas:
            ten_minutes_data[key].find_one_and_update({"DateTime":ten_minutes_datas[key]["DateTime"]},
                                                     {"$set":{"Data":ten_minutes_datas[key]["Data"]}})




def list_mean(l):
    """
    返回给定list的平均值
    :param l:
    :return:
    """
    nsum = 0
    for i in range(len(l)):
        nsum += i
    return nsum / len(l)

def high_alram_data(pool, before_time, after_time, start_time):
    """
    报警过程高采
    :param high_msg:
    :return:
    """
    r = redis.StrictRedis(connection_pool=pool)
    # 从缓存中将数据拿出，送入需要写入数据库的redis有序集合中。
    for i in range(before_time, after_time+1, 1):
        key = "High:RealData:" + str(i)
        data = r.brpop(key, timeout=20)
        if data:
            data_dict = eval(data[1])
            for key in data_dict:
                data_dict[key]["AlarmTime"] = start_time * 1000  # 将启动时间加到数据中，精确到毫秒
            r.zadd("DB:AlarmData", data_dict, i)  # 将数据放入有序集合中，成员的值是时间戳，精确到秒


def high_test_data(pool):
    """
    试验过程高采，将数据从缓存移入test_data有序集合
    :param before_time: 高采开始时间，精确到秒
    q：队列，用于传输循环停止指令
    :return:
    """
    r = redis.Redis(connection_pool=pool)
    in_test = False  # 试验标志。如为真，说明是试验中.
    start_time = None
    stop_time = None
    date_time = None  # 表示试验数据的时间，用于从redis中获取数据
    while not QUITE:
        msg = r.brpop("High:TestData", timeout=0.1)
        # 如果消息为None，并且不在试验中
        if not msg & (not in_test):
            continue
        # 如果消息非空
        if not msg:
            # 将消息转换为dict
            msg = json.loads(str(msg[1], encoding="utf-8"))
            if msg["CMD"] is "START":
                in_test = True
                start_time = msg["DateTime"] / 1000 - 30  # 启动试验前10秒，开始高采
                date_time = start_time  # 表示试验数据的时间，用于从redis中获取数据
                r.set("High:intest", True)  # 试验过程中
            elif msg["CMD"] is "STOP":
                stop_time = msg["DateTime"] / 1000

        if not in_test:
            r.set("High:intest", False)  # 试验过程中

        while in_test:

            if stop_time:  # 如果收到停止信号，等待30秒后，停止高存
                if int(time.time()) - stop_time > 30:  # 停止信号30秒后，停止高存
                    in_test = False
                    stop_time = None
            # 如果高存超过4小时，停止
            if int(time.time()) - date_time > 3600 * 4:  # 如果高采超过4小时，停止
                in_test = False
                start_time = None
            """
                    test_data的格式如下：
                    test_data = {"300MT":{
                                  "DateTime": 1,  # 数据时间，精确到秒
                                  "Frequency": 1,  # 频率，10或5000
                                  "Data":[1,2,3,4]},  # 数据，存储1秒的数据
                         "301MT":{}}
                    """
            # 从redis获取数据
            test_data = r.blpop(str(date_time), timeout=20)  # 从redis数据库中取出

            if not test_data:  # 如果数据非空
                data_dict = eval(test_data[1])  # 将数据由bytes转换为dict
                for key in data_dict:
                    data_dict[key]["TestTime"] = start_time * 1000  # 将启动时间加到数据中，精确到毫秒
                r.zadd("DB:TestData", data_dict, date_time)  # 将数据放入有序集合中，成员的值是时间戳，精确到秒
            date_time += 1






def high_alarm_analysis(pool):
    """
    分析是否根据报警信息和开关量变化信息高存数据，以及将高存数据从缓存中，挪入需要写入MongoDB的有序集合中
    :return:
    """

    r0 = redis.Redis(connection_pool=pool)

    before_start_time = None
    after_start_time = None
    in_test = False  # 是否在试验过程高存的标志
    in_high = False  # 是否在报警高存的标志
    while not QUITE:

        high_msg = r0.brpop("High:Alarm", timeout=0.2)
        high_msg = eval(high_msg[1])
        """
        high_msg = {"AlarmSource": "142XR", "StartTime": 0,"CMD":"START" }
        """
        if not high_msg:
            continue

        in_test = eval(r0.get("High:intest"))
        # 如果在试验过程中，忽略报警信息
        if in_test:
            high_msg = None
            continue


        # 情形1：如果首测启动高采.报警时间定义为t
        if not before_start_time:
            before_start_time = high_msg["StartTime"] - 10
            after_start_time = high_msg["StartTime"] + 30
        # 情形2：如果t < after_start_time
        elif (after_start_time - high_msg["StartTime"]) > 0 & (after_start_time - high_msg["StartTime"]) < 30:
            before_start_time = after_start_time
            after_start_time = high_msg["StartTime"] + 30
        # 情形3：如果 0< t - aft1er_start_time < 10
        elif (after_start_time - high_msg["StartTime"]) < 0 & (after_start_time - high_msg["StartTime"]) > -10:
            before_start_time = after_start_time
            after_start_time = high_msg["StartTime"] + 30
        elif (after_start_time - high_msg["StartTime"]) <= -10:
            before_start_time = high_msg["StartTime"] - 10
            after_start_time = high_msg["StartTime"] + 30

        # 将指定时间的数据，送入高存缓存
        high_alram_data(pool, before_start_time, after_start_time, high_msg["StartTime"],)






def high_insert_to_db(pool, client):
    """
    将高速采集的数据插入MongoDB。
    :param queues = [q1, q2, q3]: q1：OneSecondData；q2：OneMinuteData；q3：TenMinutesData
    :return:
    """

    alarm_data_db = client["AlarmData"]
    test_data_db = client["TestData"]


    r0 = redis.Redis(connection_pool=pool)


    while not QUITE:

        # 将试验时的高存数据，存入TestData数据库中
        """
        test_data = {"300MT":{"TestTime": 1,  # 试验开始时间 精确到毫秒
                              "DateTime": 1,  # 数据时间，精确到秒
                              "Frequency": 1,  # 频率，10或5000
                              "Data":[1,2,3,4]},  # 数据，存储1秒的数据
                     "301MT":{}}
        """
        test_data = r0.zrange("DB:TestData", 0,0)  # 从redis中读取数据
        if not test_data:
            test_data = eval(test_data[0])
            for key in test_data:
                test_data_db[key].insert({"TestTime":test_data[key]["TestTime"],
                                          "DateTime":test_data[key]["DateTime"],
                                          "Frequency":test_data[key]["Frequency"],
                                          "Data":test_data[key]["Data"]})
            r0.zremrangebyrank("DB:TestData", 0, 0)  # 将数据从redis的有序集合中删除

        # 将报警时的高存数据，存入AlarmData
        alarm_data = r0.zrange("DB:AlarmData", 0, 0)  # 从redis中读取数据
        if not alarm_data:
            alarm_data = eval(alarm_data[0])
            for key in test_data:
                alarm_data_db[key].insert({"AlarmTime": alarm_data[key]["AlarmTime"],
                                          "DateTime": alarm_data[key]["DateTime"],
                                          "Frequency": alarm_data[key]["Frequency"],
                                          "Data": alarm_data[key]["Data"]})
            r0.zremrangebyrank("DB:AlarmData", 0, 0)  # 将数据从redis的有序集合中删除

def info_consumer(pool):
    # 从kafka获取实时数据，放入队列，供analysis处理
    BROKER = 'dxg-ubuntu:9092'
    broker = BROKER
    # topics = settings.CMD_TOPICS
    INFO_TOPICS = ["AnalogAlarm",
                   "DynamicAlarm",
                   "TemperatureAlarm",
                   "300CRAlarm",
                   "DigitalChange"]
    topics = INFO_TOPICS
    c = Consumer({
        'bootstrap.servers': broker,
        'group.id': 'group1',
        'client.id': 'dxg',
        'default.topic.config': {
            'auto.offset.reset': 'smallest'
        }
    })

    c.subscribe(topics)
    # 创建redis实例
    r0 = redis.Redis(connection_pool=pool)

    try:
        while not QUITE:

                kafka_info = c.poll(timeout=1.0)  # 从kafka获取消息

                if kafka_info is None:
                    continue

                if kafka_info.error():
                    if kafka_info.error().code() == KafkaError._PARTITION_EOF:
                        pass
                        # print(('%% %s [%d] reached end at offset %d\n' %
                        #              (msg.topic(), msg.partition(), msg.offset())))
                    else:
                        raise KafkaException(kafka_info.error())
                if kafka_info.value() == b'Broker: No more messages':
                    continue


                # 将数据放入redis数据库
                if kafka_info.topic() == "DigitalChange":
                    r0.lpush("kafka:DigitalChange", kafka_info.value())
                else:
                    r0.lpush("kafka:Alarm", kafka_info.value())

    except KeyboardInterrupt:
        print('%% Aborted by user\n')
    finally:
        # Close down consumer to commit final offsets.
        c.close()

def info_parse(pool):


    # 保存报警开始的历史信息。内容是测点名称。如果有该测点，说明已经发生报警。待报警解除后，
    # 将测点从列表中移除
    alarm_start_list = dict()  # {"300MT":12345} 300MT:信号源；12345：StartTime
    test_start = dict()  # {"TestTime":123, "StartupTime":123} 记录试验开始时间
    r0 = redis.Redis(connection_pool=pool)

    while not QUITE:
        # 1. 处理报警信息
        alarm = r0.brpop("kafka:Alarm", timeout=0.2)
        # 将bytes类型转换为json的str类型，再转换为dict类型
        alarm = json.loads(str(alarm, encoding="utf-8"))

        for data in alarm["ListDatas"]:
            # 如果发生报警，并且该报警源不在报警列表中，说明发生报警。将报警信息写入数据库，并添加到报警列表中
            if data["AlarmState"] == 1 & (not data["AlarmSource"] in alarm_start_list):
                alarm_start_list[data["AlarmSource"]] = data["StartTime"]  # 将新增报警添加到列表中
                del data["AlarmState"]  # 为与数据库中格式一致，删除"AlarmState"
                if not ("AlarmDescription" in data):  # 如果data中没有AlarmDescription，增加该键值
                    data["AlarmDescription"] = ""
                """
                data = {"StartTime":15,"AlarmSource":"301MT","ThresholdValue":91,"AlarmValue":6, "AlarmDescription":"报警描述"}
                """
                r0.lpush("DB:AlarmLog:Start", data)  # 将信息写入redis，准备写入MongoDB
                tmp = {}
                tmp = {"AlarmSource":data["AlarmSource"], "StartTime":data["StartTime"], "CMD":"START"}
                r0.lpush("High:Alarm", data) # 送入触发高存的redis中
            # 如果没报警，但信号源在报警列表中，说明报警结束。将报警结束时间写入数据库，并将报警源从报警列表中删除
            if data["AlarmState"] == 0 & (data["AlarmSource"] in alarm_start_list):
                del alarm_start_list[data["AlarmSource"]]  # 从报警列表中删除报警源
                """
                在写入MongoDB时，根据AlarmSource找到300MT最新的一条doc，然后将EndTime写入MongoDB
                tmp = {"AlarmSource":"300MT", "EndTime":12345}
                """
                tmp = {}
                tmp["AlarmSource"] = data["AlarmSource"]
                tmp["EndTime"] = data["StartTime"]
                tmp["StartTime"] = alarm_start_list[data["AlarmSource"]]
                r0.lpush("DB:AlarmLog:End", tmp)
                tmp = {}
                tmp = {"AlarmSource": data["AlarmSource"], "StartTime": data["StartTime"], "CMD": "START"}
                r0.lpush("High:Alarm", tmp)  # 送入触发高存的redis中




        # 2. 处理开关量变化信息
        digital_change = r0.brpop("DigitalChange", timeout=0.2)
        # 将bytes类型转换为json的str类型，再转换为dict类型
        digital_change = eval(digital_change[1])
        # 将数字量变化信息，放入redis数据库，准备写入MongoDB数据库
        # data = {"134XR":{"Datetime":1534405765639,"Status":"0"}}
        for data in digital_change["ListDatas"]:
            r0.lpush("DB:DigitalLog", data)
            for key in data:
                # 判断是否开始试验
                if key is "142XR":
                    # 开始试验，并将开始时间写入MongoDB
                    if data[key]["Status"] == 1:
                        tmp = {}
                        tmp["TestTime"] = data[key]["DateTime"]
                        tmp["StartupTime"] = data[key]["DateTime"]
                        test_start = tmp
                        r0.lpush("DB:OtherData:Start", tmp)
                        #  高存控制命令写入redis
                        tmp = {}
                        tmp["CMD"] = "START"
                        tmp["DateTime"] = data[key]["DateTime"]
                        r0.lpush("High:Test", tmp)
                    else:  # 结束试验，将结束时间写入
                        test_start["StopTime"] = data[key]["DateTime"]
                        r0.lpush("DB:OtherData:Stop", test_start)
                        test_start.clear()
                else:
                    tmp = {}
                    tmp = {"StartTime": data[key]["DateTime"], "AlarmSource": key, "CMD":"START"}
                    r0.lpush("High:Alarm", tmp)  # 将报警信息写入redis数据库




def in_list(alarm_start_list, source_name):
    index = 0
    for alarm in alarm_start_list:
        if source_name == alarm["AlarmSource"]:
            break
        index += 1
    return index






def info_insert_to_db(pool, client):

    """
    将其他数据插入MongoDB。
    其他数据包括：数字量变化信息、报警信息、300CR报警信息、试验相关信息
    :param queues = [q1, q2, q3]: q1：OneSecondData；q2：OneMinuteData；q3：TenMinutesData
    :return:
    """
    r0 = redis.Redis(connection_pool=pool)
    client = pymongo.MongoClient()
    alarm_log = client["AlarmLog"]
    digital_log = client["DigitalLog"]
    other_data = client["OtherData"]

    while not QUITE:

        # 将报警信息存入数据库
        col1 = alarm_log["MainAlarm"]
        (name, alarm) = r0.brpop("DB:AlarmLog:Start", timeout=0.1)
        alarm = json.loads(str(alarm, encoding="utf-8"))
        col1.insert_one(alarm)
        # 如果报警结束，将结束时间存入数据库
        (name, alarm) = r0.brpop("DB:AlarmLog:End", timeout=0.1)
        alarm = json.loads(str(alarm, encoding="utf-8"))
        conditon = {"AlarmSource": alarm["AlarmSource"], "StartTime": alarm["StartTime"]}
        col1.find_one_and_update(conditon, {"$set":{"EndTime": alarm["EndTime"]}})

        #  将开关量变化信息存入数据库
        (name, digital) = r0.brpop("DB:DigitalLog", timeout=0.1)
        digital = json.loads(str(digital, encoding="utf-8"))
        for key in digital:
            digital_log["MainAlarm"][key].insert_one(digital[key])

        # 将统计信息存储数据库
        (name, test_info) = r0.brpop("DB:OtherData:Start", timeout=0.1)
        test_info = json.loads(str(test_info, encoding="utf-8"))
        other_data["TestTime"].insert_one(test_info)
        (name, test_info) = r0.brpop("DB:OtherData:Stop", timeout=0.1)
        test_info = json.loads(str(test_info, encoding="utf-8"))
        conditon = {"TestTime":test_info["TestTime"], "StartupTime":test_info["StartupTime"]}
        other_data["TestTime"].find_one_and_update(conditon, {"$set":{"StopTime": test_info["StopTime"]}})




def is_alarm(info, alarm):
    """
    判断从kafka获取的数据是否有报警，如果有报警。is_alarm为真，并输出报警列表
    :param info:
    :return:
    """
    alarmlist = []
    alarm = False
    for listdata in info["ListDatas"]:
        if listdata["AlarmState"] == 1:
            tmp = listdata
            tmp["DateTime"] = info["DateTime"]
            alarmlist.append(tmp)
    if len(alarmlist):
        alarm = True
    return alarm, alarmlist

if __name__ == '__main__':
    init_documents()