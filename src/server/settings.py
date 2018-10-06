#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

DBs = ["OneSecondData",
       "OneMinuteData",
       "TenMinutesData",
       "TestData",
       "DigitalLog",
       "AlarmData",
       "AlarmLog",
       "OtherData"]
OneSecondData = {"DateTime":0, "Data":}

# 功能：保存全局设定值

# kafka broker 地址
BROKER = 'dxg-ubuntu:9092'

# 121UC 地址
TCP_ADDR_122UC = ('localhost', 6340)

"""
MongoDB数据库内的数据格式
"""

DigitalLog = {"DateTime": 1537522209587,  # 时间精确到毫秒
              "Status": 0}  # 0或者1，0表示高电平跳变到低电平，1表示低电平跳变到高电平

"""
kafka的数据格式
"""
DigltalLog = {"DateTime":1534405765000,
              "ListDatas":[{"OriginName":"901XR","Time":1534405765639,"Status":"0"},
                           {"OriginName":"905XR","Time":1534405765639,"Status":"1"}]}