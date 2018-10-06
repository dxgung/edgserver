#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

import multiprocessing
import threading
import queue
import sys
import redis
from tcpclient.tcpclient import a

def run():

    try:
        pool1 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1)
        pool0 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
    except:
        print("error")
    q1 = queue.Queue()
    q2 = queue.Queue()

    p1 = multiprocessing.Process(target=tcpclient_121uc, args=(pool0, pool1))  # 进程1 从121UC读取模拟量和开关量，处理后送入kafka和redis
    p2 = multiprocessing.Process(target=tcpclient_122uc, args=(pool0, pool1))  # 进程2 从122UC读取温度信号，处理后送入kafka和redis
    p3 = multiprocessing.Process(target=ccc)  # 进程3 从kafka和redis获取数据，写入MongoDB
    p4 = multiprocessing.Process(target=ddd)  # 进程4 从kafka获取数据请求，从MongoDB读取数据，写入kafka


    p1.start()
    p2.start()



if __name__ == '__main__':
    try:
        pool1 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1)
        pool0 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
    except:
        print("error")
    run()
