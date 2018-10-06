#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate: 20180927
# Author:

"""
功能：模拟心跳监测服务端
心跳监测的客户端是device_status.py中的check_network()
"""

import socket               # 导入 socket 模块
import time
import threading
def device_status_server(port):

    s1 = socket.socket()         # 创建 socket 对象
    s1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        s1.bind(("localhost", port))  # 绑定端口
    except socket.error as e:
        print("error: ", e)

    s1.listen(5)                 # 等待客户端连接

    while True:

        print("waiting for accept: ", port)
        c, addr = s1.accept()     # 建立客户端连接。
        while True:
            revc_msg = c.recv(1024)
            if not revc_msg:
                break
            if revc_msg[0:revc_msg.rfind(b'\x0D\x0A')] == b"CheckNetwork":
                send_msg = b"CheckNetwork<ACK>" + b'\x0D\x0A'
                c.sendall(send_msg)
                print("access")

        c.close()                # 关闭连接


if __name__ == '__main__':

    t1 = threading.Thread(target=device_status_server, args=(12345,))
    t2 = threading.Thread(target=device_status_server, args=(12346,))
    t1.start()
    t2.start()