#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:


import time
import socket
import utils
import settings

def tcpserver():
    ADDR = settings.TCP_ADDR_122UC

    sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk.bind(ADDR)
    sk.listen(5)
    i = 0
    while True:

        print("connecting")
        conn, ad = sk.accept()
        while True:
            # print("connected")
            time.sleep(0.1)
            msg = utils.simulate_tcp_data()
            conn.send(msg)
        conn.close()

    sk.close()