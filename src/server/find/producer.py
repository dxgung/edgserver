#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

from confluent_kafka import Producer
import json
import queue

# from utils import simulate_producer
import settings
def simulate_cmd(cmd_topic):
    """

    CMD_TOPICS = ["HistoryAlarmCmd",  # 历史报警信息查询
                 "History300CRAlarmCmd",  # 300CR历史报警信息查询
                 "HistoryDigitalChangeCmd",  # 开关量变化信息查询
                 "HistoryCmd",  # 历史数据查询
                 "TestCmd"]  # 试验信息查询

    :param q1:
    :return:
    """
    broker = settings.BROKER
    topics = settings.CMD_TOPICS
    # 配置Producer
    conf = {'bootstrap.servers': broker}

    # 创建Producer实例
    p = Producer(**conf)

    if cmd_topic == "HistoryAlarmCmd":
        cmd = {"SourceName": "300MT",
               "StartTime": 1537668000000,  # 2018/9/23 10:00:00
               "EndTime": 1537668000000}  # 2018/9/23 10:00:00
        # cmd = {"SourceName": "All",
        #        "StartTime": 1537668000000,  # 2018/9/23 10:00:00
        #        "EndTime": 1537668000000}  # 2018/9/23 10:00:00
        j_cmd = json.dumps(cmd)
        p.produce(cmd_topic, j_cmd)  # 发送到kafka

    if cmd_topic == "History300CRAlarmCmd":
        cmd = {"SourceName": "300MT",
               "StartTime": 1537668000000,  # 2018/9/23 10:00:00
               "EndTime": 1537668000000}  # 2018/9/23 10:00:00
        # cmd = {"SourceName": "All",
        #        "StartTime": 1537668000000,  # 2018/9/23 10:00:00
        #        "EndTime": 1537668000000}  # 2018/9/23 10:00:00
        j_cmd = json.dumps(cmd)
        p.produce(cmd_topic, j_cmd)  # 发送到kafka

    if cmd_topic == "HistoryDigitalChangeCmd":
        cmd = {"SourceName": "905XR",
               "StartTime": 1537668000000,  # 2018/9/23 10:00:00
               "EndTime": 1537668000000}  # 2018/9/23 10:00:00
        # cmd = {"SourceName": "All",
        #        "StartTime": 1537668000000,  # 2018/9/23 10:00:00
        #        "EndTime": 1537668000000}  # 2018/9/23 10:00:00
        j_cmd = json.dumps(cmd)
        p.produce(cmd_topic, j_cmd)  # 发送到kafka

    if cmd_topic == "HistoryCmd":
        # 单时间
        cmd = {"SourceName": ["905XR", "906XR", "907XR"],
               "Time": [{"StartTime": 1537668000000, "EndTime": 1537668000000}]
               }
        # 双时间
        # cmd = {"SourceName": ["905XR", "906XR", "907XR"],
        #        "Time": [{"StartTime": 1537668000000, "EndTime": 1537668000000},
        #                 {"StartTime": 1537668000000, "EndTime": 1537668000000}]
        #        }
        j_cmd = json.dumps(cmd)
        p.produce(cmd_topic, j_cmd)  # 发送到kafka

    if cmd_topic == "TestCmd":
        # 单时间
        cmd = {"TestCmd": "TestInfo"}

        j_cmd = json.dumps(cmd)
        p.produce(cmd_topic, j_cmd)  # 发送到kafka

    p.poll(0)
    p.flush()



def kafka_producer(q1):
    # 将数据从队列中取出，以json格式放入kafka
    # 输入：q：队列
    broker = settings.BROKER

    # 配置Producer
    conf = {'bootstrap.servers': broker}

    # 创建Producer实例
    p = Producer(**conf)

    index = 0

    while True:
        try:
            try:
                try:
                    msg = q1.get(block=False, timeout=1)  # 从队列获取数据
                    # q.task_done()
                except queue.Empty:
                    continue
                if not msg:
                    continue

                # 提取topic信息
                topic = msg["topic"]
                # 删除topic元素
                msg.pop("topic")
                # 由于队列中保存的是dict格式，因此需要转换为json格式
                msg = json.dumps(msg)

                # 送入kafka
                p.produce(topic, msg)  # 发送到kafka

                index += 1
                print('发送的消息数量：', index)
            except BufferError as e:
                print(e)  # 保存错误信息
            p.poll(0)
            p.flush()
        except KeyboardInterrupt:
            print('%% Aborted by user\n')