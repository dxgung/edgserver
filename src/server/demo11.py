#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

import sys
import pandas as pd
import time
import utils

import threading

l = threading.Lock()

def a():
    while True:
        print("5秒后拿到锁")
        time.sleep(5)
        l.acquire()
        print("拿到锁了，等待5秒")
        time.sleep(5)
        l.release()
        print("解锁")
def b():
    while True:
        l.acquire()
        print("b is run")
        time.sleep(1)
        l.release()
t1 = threading.Thread(target=a)
t2 = threading.Thread(target=b)
t1.start()
t2.start()


