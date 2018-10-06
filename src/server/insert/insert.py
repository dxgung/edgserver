#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:
# 将数据插入数据库
import queue
import pymongo
import redis
import threading
from utils import (init_documents, temperature_data_analysis)

def insert(pool0, pool1):
    # pass
    # 初始化
    # 将数据库中的数据补齐
    # fill()


    t0 = threading.Thread(target=init_documents, args=(pool0,))
    t1 = threading.Thread(target=temperature_data_analysis, args=(pool0, pool1))

    t0.start()
    t0.join()
    t1.start()




if __name__ == '__main__':
    try:
        pool1 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1)
        pool0 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
    except:
        print("error")
    insert(pool0, pool1)