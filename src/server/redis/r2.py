#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:
import redis
import json
import pymongo
import time


pool = redis.ConnectionPool(host='localhost', port=6379, db=2)
r = redis.Redis(connection_pool=pool)
msg = {"DateTime":1537929674,  # 精确到秒
       "Datas":{"300MT":1, "301MT":2}}  # 秒级数据
print(r.setex("123", msg, 10))


"""
msg = {"DateTime":1537929674,  # 精确到秒
       "Datas":{"300MT":1, "301MT":2}}  # 秒级数据
"""
# msg = {"DateTime":1537929674,  # 精确到秒
#              "Datas":{"300MT":1, "301MT":2}}  # 秒级数据
# for i in range(10):
#     r.lpush("msg", msg)
# msg = {}
# for i in range(10):
#     time.sleep(1)
#     t = int(time.time())
#     for i in range(10):
#         Data = {}
#         name = str(i).rjust(3,"0")+"MT"
#         Data["DateTime"] = int(time.time()) * 1000
#         Data["Data"] = [0] * 10
#         Data["AlarmTime"] = 0
#         Data["Frequency"] = 10
#         msg[name] = Data
#
#     r.zadd("test_data",msg,t)
# print(r.zcard("name"))
# for i in range(5):
#     print(r.zrange("name", 0,0))
#     print(r.zremrangebyrank("name",0,0))
#
# print("OK")


