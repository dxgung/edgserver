#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

from confluent_kafka import Producer, KafkaError
import json
import queue
import time

# from utils import simulate_producer
import settings

def kafka_producer(q1):
    # 将数据从队列中取出，以json格式放入kafka
    # 输入：q：队列
    broker = settings.BROKER

    # topic：模拟量报警信息
    topic = 'CurrentTemperatureData'
    #
    topic_1 = "HighTemperatureData"

    # 配置Producer
    conf = {'bootstrap.servers': broker}
    while True:
        # 创建Producer实例
        try:
            p = Producer(**conf)
        except KafkaError as e:
            print("Producer: %s" % e)
            time.sleep(1)
            continue

        index = 0

        while True:
            try:
                try:
                    try:
                        low_data = q1.get(block=False, timeout=1)  # 从队列获取数据
                        # q.task_done()
                    except queue.Empty:
                        continue
                    if not low_data:
                        continue

                    # 由于队列中保存的是dict格式，因此需要转换为json格式
                    j_low_data = json.dumps(low_data)

                    # 送入kafka
                    p.produce(topic, j_low_data)  # 发送到kafka

                    index += 1
                    print('发送的消息数量：', index)
                except BufferError as e:
                    print("Error Produce Buffer: %s" % e)  # 保存错误信息
                p.poll(0)
                p.flush()
            except KeyboardInterrupt:
                print('%% Aborted by user\n')
