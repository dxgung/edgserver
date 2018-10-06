#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

import socket
import time
import queue
network_reconnect_time = 5  # 精确到秒
def check_network(q):
    """
    设备通讯监测。向121UC和122UC发送心跳，并将通讯状态发送给kafka
    device_status = {"DateTime":1234567,
                    "Datas":[{"121UC":0}, {"122UC":0}]}
    :param q: 发送给kafka的队列
    :return:
    """
    device_status = {}
    device_status["Datas"] = []
    while True:
        # 处理创建套接字异常
        try:
            s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error as e:
            print("Error creating socket: %s" % e)
            time.sleep(network_reconnect_time)
            continue
        # 处理连接套接字异常
        try:
            s1.connect(("localhost", 12345))
            s2.connect(("localhost", 12346))
        except socket.error as e:
            print("Error connecting to server: %s" % e)
            device_status["DateTime"] = int(time.time()) * 1000
            device_status['Datas'] = [{"121UC": 0}, {"122UC": 0}]
            # 发送设备状态信息
            try:
                # q.put(device_status)
                print(device_status)
            except:
                continue
            time.sleep(network_reconnect_time)
            continue

        is_start = False

        while True:
            buf1 = []
            buf2 = []
            start = 0

            if not is_start:
                # 处理发送数据错误
                try:
                    msg = b"CheckNetwork" + b"\x0D\x0A"
                    s1.sendall(msg)
                    s2.sendall(msg)
                    start = time.time()
                    is_start = True
                except socket.error as e:
                    print("Error sending data: %s" % e)
            # 接收网站返回的数据
            time.sleep(1)
            try:
                buf1 = s1.recv(1024)
                buf2 = s2.recv(1024)
            except socket.error as e:
                print("Error receiving data: %s" % e)

            device_status["DateTime"] = int(time.time()) * 1000

            if not len(buf1):  # 如果没有收到数据
                if time.time() - start > 5:
                    device_status['Datas'].append({"121UC": 0})
                    print("超过5秒未收到")
            else:  # 如果收到数据
                if buf1[0:buf1.rfind(b'\x0D\x0A')] == b"CheckNetwork<ACK>":
                    is_start = False
                    device_status['Datas'].append({"121UC": 2})

            if not len(buf2):  # 如果没有收到数据
                if time.time() - start > 5:
                    device_status['Datas'].append({"122UC": 0})
                    print("超过5秒未收到")
            else:  # 如果收到数据
                if buf2[0:buf2.rfind(b'\x0D\x0A')] == b"CheckNetwork<ACK>":
                    is_start = False
                    device_status['Datas'].append({"122UC": 2})

            # 发送设备状态信息
            try:
                # q.put(device_status)
                print(device_status)
            except:
                continue



if __name__ == '__main__':
    q = queue.Queue()
    check_network(q)