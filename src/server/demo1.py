#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:
import redis
import json
import time

r = redis.Redis("127.0.0.1", 6379)

# key = "a"
# for i in range(10):
#     r.lpush(key,i)
value = [i for i in range(100)]
start = time.time()
for i in range(10):
    name = str(i).rjust(3,"0")+"MT"
    r.lpush(name, 1)
print(time.time()-start)
# for i in range(100):
#
#     for i in range(10):

# print(value)






