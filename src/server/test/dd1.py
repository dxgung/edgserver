#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

import redis
import time
import threading
import socket
while True:
    try:
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as e:
        print("Error creating socket: %s" % e)
        time.sleep(1)
        continue

    # 处理连接套接字异常
    try:
        ss.connect(('192.168.122.32', 10345))
    except socket.error as e:
        print("Error connecting to server: %s" % e)
        time.sleep(1)
        continue

    while Ture
