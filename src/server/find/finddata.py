#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:
# 主进程
# 从kafka获取实时数据，并存入MongoDB。从redis获取高速数据，存入MongoDB。从redis获取离线数据存入MongoDB

from multiprocessing import Queue, Lock, Process
import time
import threading
import queue
import producer
from utils import sim
from consumer import kafka_consumer


q1 = queue.Queue()
t1 = threading.Thread(target=producer, args=(q1,))
t2 = threading.Thread(target=kafka_consumer, args=(q1,))

t1.start()
t2.start()