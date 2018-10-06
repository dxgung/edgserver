#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

from pymongo import MongoClient
import time
from utils import create_db

client = MongoClient()
db = client["AlarmLog"]
# for i in range(100):
#     l = list()
#     for i in range(10):
#         d = dict()
#         d["DateTime"] = 1
#         d["Data"] = [1,2,3,4,5]
#         l.append(d)
#     db["011MT"].insert_many(l)
# start = time.time()
# db["010MT"].ensure_index('DateTime')
# print(time.time()-start)
# create_db("AlarmLog")
col = db["MainAlarm"]
cmd = dict()
cmd["SourceName"] = "300MT"
cmd["StartTime"] = 1537495191
cmd["EndTime"] = 1537495205

if cmd["SourceName"] == "All":
    conditions = [{"StartTime": {"$lte": cmd["EndTime"]}, "EndTime": {"$gte": cmd["EndTime"]}},
                  {"StartTime": {"$lte": cmd["StartTime"]}, "EndTime": {"$gte": cmd["EndTime"]}},
                  {"StartTime": {"$gte": cmd["StartTime"]}, "EndTime": {"$gte": cmd["StartTime"]}}]
else:
    conditions = [{"StartTime": {"$gte": cmd["StartTime"]},
                   "StartTime": {"$lte": cmd["EndTime"]},
                   "AlarmSource": cmd["SourceName"]},
                  {"StartTime": {"$lte": cmd["StartTime"]},
                   "EndTime": {"$gte": cmd["EndTime"]},
                   "AlarmSource": cmd["SourceName"]},
                  {"EndTime": {"$gte": cmd["StartTime"]},
                   "EndTime": {"$lte": cmd["EndTime"]},
                   "AlarmSource": cmd["SourceName"]}]
results = list()
for condition in conditions:
    docs = col.find(condition,{"_id":0, "AlarmDescription":0})
    for doc in docs:
        results.append(doc)
print(results)
date_time = int(time.time())
msg = dict()  # 返回给kafka的数据
msg["DateTime"] = date_time
msg["ListDatas"] = list()
source_names = set()
tmp = dict()
tmp["SourceName"] = cmd["SourceName"]
tmp["List"] = list()
for result in results:
    del result["AlarmSource"]
    tmp["List"].append(result)
msg["ListDatas"].append(tmp)
print(msg)








