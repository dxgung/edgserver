#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

import queue
import threading
import sys
import redis

import time
from multiprocessing import Process

#
# from client import crio_client
# from tcpserver import tcpserver
# from process import data_process
# from device_status import check_network
from utils import (get_122uc,
                   data_temperature_process,
                   data_relay_process,
                   kafka_producer,
                   kafka_consumer)

sys.path.append("/root/root/usr/open/edgserver/src")
if True:
    from settings import BROKER, TCP_ADDR_122UC

def tcpclient_122uc(pool0, pool1):
    t0 = threading.Thread(target=get_122uc, args=(pool1, TCP_ADDR_122UC,))  # 从服务器获取数据
    t1 = threading.Thread(target=data_temperature_process, args=(pool1,))
    t2 = threading.Thread(target=data_relay_process, args=(pool1, pool0))
    t3 = threading.Thread(target=kafka_producer, args=(pool1,))
    t4 = threading.Thread(target=kafka_consumer)
    # t2 = threading.Thread(target=data_process, args=(q1,))  # 对数据进行处理，再送入队列
    # # t2 = threading.Thread(target=kafka_producer, args=(q1,))
    # # t3 = threading.Thread(target=analysis, args=(q1,q2,))
    # t4 = threading.Thread(target=kafka_producer, args=(q2,))  # 从队列中取出数据，送入kafka
    # t5 = threading.Thread(target=check_network, args=(q4,))  # 心跳监测

    t0.start()
    t1.start()
    t2.start()
    # t3.start()
    # t4.start()





if __name__ == '__main__':
    try:
        pool1 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1)
        pool0 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
    except:
        print("error")

    # 清空redis数据库所有key
    r = redis.Redis(host='127.0.0.1', port=6379)
    r.flushall()


    q1 = queue.Queue(10)  # 队列中数据的格式为字典
    q2 = queue.Queue(10)  # 队列中数据的格式为字典。如果要送给kafka，需要转换为json
    q3 = queue.Queue(10)
    q4 = queue.Queue(10)  # 心跳监测数据，发往kafka

    # addr = settings.TCP_ADDR_122UC


    t0 = threading.Thread(target=get_122uc, args=(pool1, TCP_ADDR_122UC,))  # 从服务器获取数据
    t1 = threading.Thread(target=data_temperature_process, args=(pool1,))
    t2 = threading.Thread(target=data_relay_process, args=(pool1,pool0))
    t3 = threading.Thread(target=kafka_producer, args=(pool1,))
    t4 = threading.Thread(target=kafka_consumer)
    # t2 = threading.Thread(target=data_process, args=(q1,))  # 对数据进行处理，再送入队列
    # # t2 = threading.Thread(target=kafka_producer, args=(q1,))
    # # t3 = threading.Thread(target=analysis, args=(q1,q2,))
    # t4 = threading.Thread(target=kafka_producer, args=(q2,))  # 从队列中取出数据，送入kafka
    # t5 = threading.Thread(target=check_network, args=(q4,))  # 心跳监测


    t0.start()
    t1.start()
    t2.start()
    # t3.start()
    # t4.start()
    # t1.start()
    # t2.start()
    #
    # # t3.start()
    # t4.start()
