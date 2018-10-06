#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:
import pymongo
import time
import datetime

from settings import TEMPERATURE, ANALOG, DIGITAL

# 初始化数据库
def init_db():
    """
    该函数仅用于对数据库进行初始化操作。将数据库历史数据删除，再以当前时间的小时为时间戳，用0填充。
    初始化的数据库包括："OneSecondData"，"OneMinuteData"，"TenMinutesData"
    :return:
    """
    one_second_data = [0]*3600
    one_minute_data = [0]*60
    ten_minute_data = [0]*6

    client = pymongo.MongoClient()
    dbs = [client["OneSecondData"], client["OneMinuteData"], client["TenMinutesData"]]
    DateTime = get_timestamp()  # 获取当前小时的时间戳
    datas = [one_second_data, one_minute_data, ten_minute_data]
    i = 0
    for db in dbs:
        client.drop_database(db.name)  #删除数据库
        doc = {"DateTime":DateTime, "Datas":datas[i]}
        i += 1
        for key in TEMPERATURE:
            res = db[TEMPERATURE[key]].find_one_and_replace({"DateTime":doc["DateTime"]}, doc)
            if not res:
                db[TEMPERATURE[key]].insert(doc)
        for key in ANALOG:
            res = db[ANALOG[key]].find_one_and_replace({"DateTime":doc["DateTime"]}, doc)
            if not res:
                db[ANALOG[key]].insert(doc)
        for key in DIGITAL:
            res = db[DIGITAL[key]].find_one_and_replace({"DateTime": doc["DateTime"]}, doc)
            if not res:
                db[DIGITAL[key]].insert(doc)




def get_timestamp():
    # 返回以小时为精度的时间戳

    # 获取当前时间
    dt = datetime.datetime.now().strftime("%Y-%m-%d %H")
    # 转换为时间数组
    timeArray = time.strptime(dt, "%Y-%m-%d %H")
    # 按小时转换为时间戳
    timestamp = int(time.mktime(timeArray))
    return timestamp


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


if __name__ == '__main__':
    init_db()
