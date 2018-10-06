#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:
import socket
import time
import struct
import queue

# import utils

def aaaa():
    print("client a")

def crio_client(addr,q):
    """
    功能：连接cRIO提供的TCP服务器
    :param addr: TCP服务器的IP地址，数据类型是元组。例如('localhost', 6340)
    :param q: 队列，保存采集到的数据。数据类型为dict
    :return:
    """
    while True:
        # 处理创建套接字异常
        try:
            ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error as e:
            print("Error creating socket: %s" % e)
            time.sleep(1)
            continue
        # 处理连接套接字异常
        try:
            ss.connect(("localhost", 12345))
        except socket.error as e:
            print("Error connecting to server: %s" % e)
            time.sleep(1)
            continue


    buffer = list()

    while True:
        # print("开始接收数据")
        try:
            d = ss.recv(10240)
        except socket.error as e:
            print("Error receiving: %s" % e)
            break

        if not d:
            continue

        header = d[0:2]  # 帧头

        (byte_num,) = struct.unpack('<l', d[2:6])  # 数据包字节数：4字节；U32

        (serial_num,) = struct.unpack('<l', d[6:10])  # 数据包流水号；4字节；U32

        (dataframe_type,) = struct.unpack('<H', d[10:12])  # 数据包类型；2字节；U16

        (frequency,) = struct.unpack('<f', d[12:16])  # 采样频率；4字节；SGL
        #  datetime：时间戳，精确到秒；microsecond：毫秒，毫秒取值是0,100,...,900
        (datetime, microsecond) = utils.date_struct(d[16:24])  # 时间信息；8字节；

        (channel_num,) = struct.unpack('<H', d[24:26])  # 通道数量；2字节；U16

        (str_length,) = struct.unpack('<H', d[26:28])  # 通道字符串长度；2字节；U16
        # channel_names: ["300MT", "301MT",...,"323MT"]
        channel_names = utils.get_channel_name_struct(d[28:(28 + str_length)])  # 通道名称列表

        byte_datas = d[(28 + str_length):byte_num-2]


        #  处理仿真数据  如果是真实数据的话，应该使用下面注释的代码
        # datas = utils.byte_data(byte_datas, channel_num)


        #  处理真实数据
        byte_datas = d[(28 + str_length):byte_num]
        (package_num,) = struct.unpack('<i', byte_datas[0:4])  # 数据包的个数；低采样时为1；高采样时模拟量为1000，其他为1
        (channel_count,) = struct.unpack('<i', byte_datas[4:8])  # 每个数据包中通道的数量
        datas = list()
        for i in range(package_num):
            start_byte = 8 + i * channel_count * 4
            end_byte = 8 + (i + 1) * channel_count * 4
            datas.append(utils.byte_data(byte_datas[start_byte:end_byte], channel_count))


        tail = d[byte_num - 2:byte_num]
        d_data = {"header": header,
                  "byte_num": byte_num,
                  "serial_num": serial_num,
                  "dataframe_type": dataframe_type,
                  "frequency": frequency,
                  "datetime": datetime,
                  "microsecond": microsecond,
                  "channel_num": channel_num,
                  "str_length": str_length,
                  "channel_names": channel_names,
                  "realdata": datas,
                  "tail": tail}

        try:
            q.put(d_data, block=False)
            # print("d_data: ", d_data)
            # print("datetime", datetime)
        except:
            continue



if __name__ == '__main__':
    q = queue.Queue()
    crio_client(('localhost', 6340), q)