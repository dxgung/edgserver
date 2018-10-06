#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

# 保存查询命令的topic
CMD_TOPICS = ["HistoryAlarmCmd",  # 历史报警信息查询
              "History300CRAlarmCmd",  # 300CR历史报警信息查询
              "HistoryDigitalChangeCmd",  # 开关量变化信息查询
              "HistoryCmd",  # 历史数据查询
              "TestCmd"]  # 试验信息查询

# 保存查询结果的topic
MSG_TOPICS = ["HistoryAlarm",  # 历史报警信息
              "History300CRAlarm",  # 300CR历史报警信息
              "HistoryDigitalChange",  # 开关量变化信息查询
              "HistoryData",  # 历史数据查询
              "TestInfo"]  # 试验信息

# kafka broker 地址
BROKER = 'dxg-ubuntu:9092'