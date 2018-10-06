#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

import producer
import settings
import time
import json
import sys
import pymongo


client = pymongo.MongoClient()
db = client["AlarmLog"]
l = list()
for i in range(1000000):
    l.append(i)

a = {}

a_size = sys.getsizeof(l)
print(a_size/1024/1024)





