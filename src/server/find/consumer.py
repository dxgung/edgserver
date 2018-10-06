#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

from confluent_kafka import Consumer, KafkaError, KafkaException
import queue
import json
import time

import settings
import utils


def print_assignment(consumer, partitions):
    print('Assignment:', partitions)

def kafka_consumer(q):
    # 从kafka获取实时数据，放入队列，供analysis处理
    BROKER = 'dxg-ubuntu:9092'
    broker = BROKER
    topics = settings.CMD_TOPICS
    CMD_TOPICS = ["HistoryAlarmCmd",  # 历史报警信息查询
                  "History300CRAlarmCmd",  # 300CR历史报警信息查询
                  "HistoryDigitalChangeCmd",  # 开关量报警信息查询
                  "HistoryCmd",  # 历史数据查询
                  "TestCmd"] # 试验信息查询


    c = Consumer({
        'bootstrap.servers': broker,
        'group.id': 'group1',
        'client.id': 'dxg',
        'default.topic.config': {
            'auto.offset.reset': 'smallest'
        }
    })

    c.subscribe(topics)

    try:
        while True:
                kafka_cmd = c.poll(timeout=1.0)  # 从kafka获取消息

                if kafka_cmd is None:
                    continue

                if kafka_cmd.error():
                    if kafka_cmd.error().code() == KafkaError._PARTITION_EOF:
                        pass
                        # print(('%% %s [%d] reached end at offset %d\n' %
                        #              (msg.topic(), msg.partition(), msg.offset())))
                    else:
                        raise KafkaException(kafka_cmd.error())
                if kafka_cmd.value() == b'Broker: No more messages':
                    continue

                # 将bytes类型转换为json的str类型
                cmd = str(kafka_cmd.value(), encoding="utf-8")
                # 将str类型转换为dict类型
                cmd = json.loads(cmd)

                # 根据查询命令，到数据库中查询，并返回查询结果。类型是dict
                msg = utils.find(kafka_cmd.topic(), cmd)

                # 将返回的消息发给producer
                try:
                    q.put(msg, block=False)
                except queue.Full:
                    continue
    except KeyboardInterrupt:
        print('%% Aborted by user\n')
    finally:
        # Close down consumer to commit final offsets.
        c.close()

if __name__ == '__main__':
    q = queue.Queue()
    kafka_consumer(q)