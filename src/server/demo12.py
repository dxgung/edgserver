#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:
import time
import random
import queue
import utils
import find
# utils.create_db("DigitalLog")
cmds = {}
conditions = [{"DateTime": {"$gte": 1537579099096, "$lte": 1537582949212}}]
cmds["topic_name"] = "HistoryDigitalChangeCmd"
cmds["database_name"] = "DigitalLog"
cmds["source_name"] = "531XR"
cmds["conditions"] = conditions


res = find.find_history_digital_change(cmds)
print(res)


# for d in ds:
#     if not ss:
#         ss = d["a"]
#     if ss == d["a"]:
#         dd["SourceName"] = ss
#         dd["List"].append(d["x"])
#     else



# {"datetime":1536836517102,
#  "listDatas":[{"SourceName":"300MV","list":[{"alarmValue":0.922205031,"endTime":1536807523780,"startTime":1536807034934,"thresholdValue":0.0},{"alarmValue":12.3590384,"endTime":1536808680028,"startTime":1536808640884,"thresholdValue":0.0}]},
#               {"SourceName":"157MP","list":[{"alarmValue":0.00682106,"endTime":1536807545194,"startTime":1536807059189,"thresholdValue":0.0}]}]}