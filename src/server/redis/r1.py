#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

import redis
import json
import time
import threading

pool = redis.ConnectionPool(host='localhost', port=6379, db=1)
r1 = redis.Redis(connection_pool=pool)
r2 = redis.Redis(connection_pool=pool)

def push(r):
    while True:
        for i in range(50):
            print("push: ", i)
            r.lpush("ddd", i)
            time.sleep(0.5)
def pop(r):
    while True:
        print(r.brpop("ddd", timeout=1))
        time.sleep(0.1)

t1 = threading.Thread(target=push, args=(r1,))
t2 = threading.Thread(target=pop, args=(r2,))
t1.start()
time.sleep(1)
# t2.start()

# key = "a"
# for i in range(10):
#     r.lpush(key,i)
# value = [i for i in range(100)]
# start = time.time()
# for i in range(10):
#     name = str(i).rjust(3,"0")+"MT"
#     r.lpush(name, i)
# print(time.time()-start)
# for i in range(100):
#
#     for i in range(10):

# print(value)
# print(r.blpop(["002MT", "000MT"], timeout=5))
