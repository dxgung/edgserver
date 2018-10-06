#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:
import struct
import datetime
import numpy as np
import pandas as pd
import random
import sys
from multiprocessing import Queue
import time
import socket
import redis
import json
from confluent_kafka import Producer, KafkaError, Consumer, KafkaException


sys.path.append("/root/root/usr/open/edgserver/src")
if True:
    from settings import BROKER, TCP_ADDR_122UC, TEMPERATURE, ANALOG, DIGITAL

QUITE = False



def simulate_tcp_data():
    # 模拟产生tcp的数据

    # 模拟温度数据包
    header = bytes(b'\x50\x50')  # 帧头 2字节

    serial_num = struct.pack('<l', 12)  # 数据包流水号；4字节；U32
    dataframe_type = struct.pack('<H', 0)  # 数据包类型；2字节；U16  122UC的温度采集信号为0，报警信号为1
    frequency = struct.pack('<f', 10)  # 采样频率；4字节；SGL
    # 时间信息
    now = datetime.datetime.now()
    year = struct.pack('<H', now.year)  # 年；2字节；U16
    month = struct.pack('<B', now.month)  # 月；1字节；U8
    day = struct.pack('<B', now.day)  # 月；1字节；U8
    hour = struct.pack('<B', now.hour)  # 月；1字节；U8
    minute = struct.pack('<B', now.minute)  # 月；1字节；U8
    second = struct.pack('<H', now.second*1000+now.microsecond // 1000)  # 毫秒；2字节；U16

    # 通道名称
    channel_num = struct.pack('<H', 31)  # 通道数  温度信号的通道数量为31
    str_length = struct.pack('<H', 185)  # 通道字符串长度；2字节；U16 每个通道名称的长度是5

    c_name = ""
    for i in range(31):
        c_name += "3"+str(i).rjust(2, "0")+"MT"
        if i < 30:
            c_name += ","
    channel_names = bytes(c_name, encoding='utf8')

    # 数据
    byte_datas = bytes()
    for i in range(31):
        byte_datas += struct.pack('<f', random.random()*100)

    # 帧尾
    tail = bytes(b'\x0D\x0A')  # 帧尾 2字节



    tcp_frame_main = (serial_num + dataframe_type + frequency + year +
                      month + day + hour + minute + second +
                      channel_num + str_length + channel_names + byte_datas)
    byte_num = struct.pack('<l', len(tcp_frame_main) + 2 + 4 + 2)
    tcp_frame = header + byte_num + tcp_frame_main + tail

    return tcp_frame

CHANNEL_NAMES = pd.Series(['4LHP300MT', '4LHP301MT'], index=['9426-1-CH0', '9426-1-CH1'])

def Bin2Data(bin, binlength):
    # 将二进制字节转换为数组
    chs = []
    print('start')
    for i in range(binlength):
        k = i * 8
        j = i * 8 + 8
        ch = bin[k:j]
        ret, = struct.unpack("<f", ch)  # SGL
        chs.append(ret)
    chs = np.array(chs)
    print('end')
    return chs

def byte_to_data(bin, binlength):
    # 将二进制字节转换为数组
    chs = []
    for i in range(binlength):
        k = i * 4
        j = i * 4 + 4
        ch = bin[k:j]
        ret, = struct.unpack("<f", ch)      # SGL
        chs.append(round(ret,1))
    return chs


def date_struct(bytes):
    # 将二进制的日期转换为字符串
    (year,) = struct.unpack('<H', bytes[0:2])  # U16
    (month,) = struct.unpack('<B', bytes[2:3])
    (day,) = struct.unpack('<B', bytes[3:4])
    (hour,) = struct.unpack('<B', bytes[4:5])
    (minute,) = struct.unpack('<B', bytes[5:6])
    # (second,) = struct.unpack('<B', bytes[6:7])
    # (microsecond,) = struct.unpack('<H', bytes[7:9])
    (second2,) = struct.unpack('<H', bytes[6:8])  # 5.2秒=5200毫秒 second2=5200
    second = second2//1000
    microsecond = int(second2/100%10)*100  # 获得毫秒数，并转换为0,100,...,900

    dt = datetime.datetime(
                year=year,
                month=month,
                day=day,
                hour=hour,
                minute=minute,
                second=second
                # microsecond=microsecond
            )
    # dt.strftime("%Y-%m-%d %H:%M:%S")
    un_time = int(time.mktime(dt.timetuple()))  # 将datetime转换为时间戳
    return (un_time, microsecond)

def read_dict_csv(fileName="", keyIndex=0, valueIndex=1):
    # 从csv中读取通道信息
    dataDict = {}

def channel_info_struct(bytes):
    # 返回通道信息
    channel_count = struct.unpack('<H', bytes[0:2])
    str_longth = struct.unpack('<H', bytes[2:4])
    channels = bytes
def get_channel_count_struct(bytes):
    # 返回通道数量
    return struct.unpack('<H', bytes[0:2])

def length_struct(bytes):
    # 返回通道名总长度
    return struct.unpack('<H', bytes[0:2])

def get_channel_name_struct(bytes):
    # 返回通道名称，将9213-1-CH0转换为4LHP300MT
    device_names = list()
    str = bytes.decode()  # 将bytes转换为str
    io_names = str.split(',')  # 按','分隔符提取通道名称
    device_names = io_names
    # # 将9213-1-CH0转换为4LHP300MT
    # for cn in io_names:
    #     device_names.append(CHANNEL_NAMES[cn])
    return device_names

def get_microsecond(microsecond):
    # 将microsecond归一化为整百毫秒
    # 输入：1-999
    # 输出：归一化后的毫秒：0,100,200,300,400,500,600,700,800,900
    return (microsecond // 100) * 100

def send(q, topic):
    # 将数据送入kafka
    # q: 数据队列
    # topic: kafka中topic的名称
    print(topic)
    while True:
        # 连接kafka
        pass
        # 如果连接不成功，等待2秒，继续尝试连接
        time.sleep(1)
        # 连接成功后，发送数据
        try:
            data = q.get()
            print(data)
            if q.empty():
                break
        except:
            break

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

def check_network():
    ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ss.connect(("localhost", 6340))
    buffer = list()

    while True:
        d = ss.recv(10240)



def get_121uc(pool, addr):
    """
    功能：连接cRIO提供的TCP服务器
    :param addr: TCP服务器的IP地址，数据类型是元组。例如('localhost', 6340)
    :param q: 队列，保存采集到的数据。数据类型为dict
    :return:
    """
    digital = DIGITAL
    analog = ANALOG


    while not QUITE:
        # 处理创建套接字异常
        try:
            ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except socket.error as e:
            print("Error creating socket: %s" % e)
            time.sleep(1)
            continue
        # 处理连接套接字异常
        try:
            ss.connect(addr)
        except socket.error as e:
            print("Error connecting to server: %s" % e)
            time.sleep(1)
            continue
        # 连接到redis
        r = redis.StrictRedis(connection_pool=pool)

        buffer = list()

        while not QUITE:
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
            (datetime, microsecond) = date_struct(d[16:24])  # 时间信息；8字节；

            (channel_num,) = struct.unpack('<H', d[24:26])  # 通道数量；2字节；U16

            (str_length,) = struct.unpack('<H', d[26:28])  # 通道字符串长度；2字节；U16
            # channel_names: ["300MT", "301MT",...,"323MT"]
            channel_names = get_channel_name_struct(d[28:(28 + str_length)])  # 通道名称列表
            if dataframe_type == 0:
                channel_names = [digital[key] for key in channel_names]
            elif dataframe_type == 1:
                channel_names = [analog[key] for key in channel_names]

            byte_datas = d[(28 + str_length):byte_num-2]

            package_num = int(len(byte_datas) / (len(channel_names) * 4))

            datas = list()

            for i in range(package_num):
                start_byte = i * len(channel_names) * 4
                end_byte = (i + 1) * len(channel_names) * 4
                datas.append(byte_to_data(byte_datas[start_byte:end_byte], len(channel_names)))

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
                if d_data["dataframe_type"] == 0:  # 0是数字量
                    j_d_data = json.dumps(d_data)
                    r.lpush("CRIO:digital", j_d_data)
                elif d_data["dataframe_type"] == 1:
                    j_d_data = json.dumps(d_data)
                    r.lpush("CRIO:analog", j_d_data)
            except:
                continue


def get_122uc(pool, addr):
    """
    功能：连接cRIO提供的TCP服务器
    :param addr: TCP服务器的IP地址，数据类型是元组。例如('localhost', 6340)
    :param q: 队列，保存采集到的数据。数据类型为dict
    :return:
    """
    temperature = TEMPERATURE


    while not QUITE:
        # 处理创建套接字异常
        try:
            ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except socket.error as e:
            print("Error creating socket: %s" % e)
            time.sleep(1)
            continue
        # 处理连接套接字异常
        try:
            ss.connect(addr)
        except socket.error as e:
            print("Error connecting to server: %s" % e)
            time.sleep(1)
            continue
        try:
            # 连接到redis
            r = redis.StrictRedis(connection_pool=pool)
        except:
            print("r error")

        buffer = list()

        while not QUITE:
            # print("开始接收数据")
            try:
                d = ss.recv(10240)
            except socket.error as e:
                print("Error receiving: %s" % e)
                break

            if not d:
                continue
            header = str(d[0:2], encoding="utf-8")  # 帧头

            (byte_num,) = struct.unpack('<l', d[2:6])  # 数据包字节数：4字节；U32

            (serial_num,) = struct.unpack('<l', d[6:10])  # 数据包流水号；4字节；U32

            (dataframe_type,) = struct.unpack('<H', d[10:12])  # 数据包类型；2字节；U16

            (frequency,) = struct.unpack('<f', d[12:16])  # 采样频率；4字节；SGL
            #  datetime：时间戳，精确到秒；microsecond：毫秒，毫秒取值是0,100,...,900
            (datetime, microsecond) = date_struct(d[16:24])  # 时间信息；8字节；

            (channel_num,) = struct.unpack('<H', d[24:26])  # 通道数量；2字节；U16

            (str_length,) = struct.unpack('<H', d[26:28])  # 通道字符串长度；2字节；U16
            # channel_names: ["300MT", "301MT",...,"323MT"]
            channel_names = get_channel_name_struct(d[28:(28 + str_length)])  # 通道名称列表
            if dataframe_type == 1:
                channel_names = [temperature[key] for key in channel_names]

            byte_datas = d[(28 + str_length):byte_num-2]

            package_num = int(len(byte_datas)/(len(channel_names)*4))
            datas = list()

            for i in range(package_num):
                start_byte = i * len(channel_names) * 4
                end_byte = (i + 1) * len(channel_names) * 4
                datas.append(byte_to_data(byte_datas[start_byte:end_byte], len(channel_names)))


            tail = str(d[byte_num - 2:byte_num], encoding='utf-8')
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
                if d_data["dataframe_type"] == 1:  # 1是实时温度值
                    # 注意：
                    j_d_data = json.dumps(d_data)
                    res = r.lpush("CRIO:temperature", j_d_data)
                elif d_data["dataframe_type"] == 4:  # 4是继电器状态
                        j_d_data = json.dumps(d_data)
                        r.lpush("CRIO:relay", j_d_data)
            except :
                continue



def get_realdata():
    """
    仿真产生真实数据
    :return:
    """
    realdata = list(dict())
    for i in range(40):
        t = time.time()
        rd = np.random.rand(4).tolist()
        d = {'datetime':int(t),
             'microsecond': (t - int(t)) * 100,
             'channel_names': ['300MT', '301MT', '302MT', '303MT'],
             'realdata': rd
             }
        realdata.append(d)
        time.sleep(0.1)
    return realdata


def average_list(data):
    """
    返回列表的平均值
    :param data: 列表
    :return: 列表的平均值
    """
    dsum = 0
    for i in range(len(data)):
        dsum += data[i]
    return dsum / len(data)

def data_analog_process(pool):
    """
    对数据进行处理，从redis中读取数据，处理后，秒级数据放入OneSecond,；原始数据（高频数据）放入High
    :param pool: redis数据库
    :return:
    """
    #  温度的高存数据保存在db=1中，模拟量保存在db=2中，开关量保存在db=3中
    r = redis.StrictRedis(connection_pool=pool)
    current_second = None
    data_type = "analog"
    """
    20180927
    增加one_second_data的格式
    one_second_data = {"DateTime":1, "Datas":{"300MT":1, "301MT":1}}
    """
    one_second_data = dict()  # 存储秒级数据

    """
    20180927
    增加high_data的格式
    high_data = {"300MT": {"DateTime": 1, "Frequency": 1, "Datas": [1, 2, 3, 4]},
                 "301MT": {"DateTime": 1, "Frequency": 1, "Datas": [1, 2, 3, 4]}}
    """
    high_data = dict()  # 存储高存数据
    data_list = []  # 存储1秒内的所有数据

    index = 0
    while not QUITE:
        try:
            key = "CRIO:" + data_type
            data = r.brpop(key, timeout=1)
        except:
            continue
        if not data:
            continue
        else:
            real_datas = eval(data[1])

        # 对current_second和high_data进行初始化
        if not current_second:
            for key in real_datas['channel_names']:
                high_data[key] = {}
                high_data[key]["Datas"] = []

            current_second = real_datas['datetime']


        # 对于模拟量，每个数据包中有500组数据。
        # 将数据持续的送入data_list，当超过1秒钟，就把数据从data_list中取出，同时清空data_list
        for rd in real_datas['realdata']:
            data_list.append(rd)


        if current_second != real_datas['datetime']:
        # 如果不是同一秒的数据，对数据进行预处理，送入kafka和redis
            one_second_data['DateTime'] = current_second * 1000  # 精确到毫秒
            dd = {}
            date_time = current_second * 1000
            # 将秒级数据求平均，然后转换为字典{'4LHP300MT':300, '4LHP301MT':301}

            data_array = np.array(data_list)  # 将列表转换为二维数组
            data_mean = np.mean(data_array, dtype=np.float16, axis=0).tolist()  # 对二维列表求均值，再转为列表
            i = 0
            for key in real_datas['channel_names']:
                dd[key] = data_mean[i]
                high_data[key] = {}
                high_data[key]["DateTime"] = date_time
                high_data[key]["Frequency"] = real_datas['frequency']
                high_data[key]["Datas"] = data_array[..., i]
                i += 1


            one_second_data['Datas'] = dd

            # 将1秒的数据送入redis数据库
            try:
                key = "OneSecond:" + data_type
                r.lpush(key, one_second_data)
            except:
                continue

            # 将高存数据送往redis高速缓存
            try:
                key = "High:" + data_type + ":" + str(current_second)
                life_time = 50  # 生存时间
                r.setex(key, life_time, high_data)  # 将数据保存到redis中，生存时间50秒。时间到了之后，会自动删除。
            except:
                continue

            # 变量清空
            one_second_data.clear()  # 清空字典
            # 初始化
            current_second = real_datas['datetime']
            data_list = []


def data_temperature_process(pool):
    """
    对数据进行处理，从redis中读取数据，处理后，秒级数据放入OneSecond,；原始数据（高频数据）放入High
    :param pool: redis数据库
    :return:
    """

    r = redis.StrictRedis(connection_pool=pool)
    current_second = None
    data_type = "temperature"
    """
    20180927
    增加one_second_data的格式
    one_second_data = {"DateTime":1, "Datas":{"300MT":1, "301MT":1}}
    """
    one_second_data = dict()  # 存储秒级数据

    """
    20180927
    增加high_data的格式
    high_data = {"300MT": {"DateTime": 1, "Frequency": 1, "Datas": [1, 2, 3, 4]},
                 "301MT": {"DateTime": 1, "Frequency": 1, "Datas": [1, 2, 3, 4]}}
    """
    high_data = dict()  # 存储高存数据
    data_list = []  # 存储1秒内的所有数据

    index = 0
    while not QUITE:
        try:
            key = "CRIO:" + data_type
            data = r.brpop(key, timeout=1)
        except:
            print("error")
            continue

        if not data:
            print("empty")
            continue
        else:
            real_datas = eval(data[1])

        # 对current_second和high_data进行初始化
        if not current_second:
            for key in real_datas['channel_names']:
                high_data[key] = {}
                high_data[key]["Datas"] = []

            current_second = real_datas['datetime']

        # 对于温度，每个数据包中有1组数据。
        # 将数据持续的送入data_list，当超过1秒钟，就把数据从data_list中取出，同时清空data_list
        for rd in real_datas['realdata']:
            data_list.append(rd)
        if current_second != real_datas['datetime']:
        # 如果不是同一秒的数据，对数据进行预处理，送入kafka和redis
            one_second_data['DateTime'] = current_second * 1000  # 精确到毫秒
            dd = {}
            date_time = current_second * 1000
            # 将秒级数据求平均，然后转换为字典{'4LHP300MT':300, '4LHP301MT':301}
            # print("data_list:", data_list)
            data_array = np.array(data_list)  # 将列表转换为二维数组
            data_mean = np.mean(data_array, dtype=np.float16, axis=0).tolist()  # 对二维列表求均值，再转为列表
            i = 0
            for key in real_datas['channel_names']:
                dd[key] = data_mean[i]
                high_data[key] = {}
                high_data[key]["DateTime"] = date_time
                high_data[key]["Frequency"] = real_datas['frequency']
                high_data[key]["Datas"] = data_array[..., i]
                i += 1
            one_second_data['Datas'] = dd
            # 将1秒的数据送入redis数据库
            try:
                key = "OneSecond:" + data_type
                r.lpush(key, one_second_data)
            except:
                continue
            # 将高存数据送往redis高速缓存
            try:
                key = "High:" + data_type + ":" + str(current_second)
                life_time = 50  # 生存时间
                r.setex(key, life_time, high_data)  # 将数据保存到redis中，生存时间50秒。时间到了之后，会自动删除。
            except:
                continue

            # 变量清空
            one_second_data.clear()  # 清空字典
            # 初始化
            current_second = real_datas['datetime']
            data_list = []




def data_digital_process(pool0, pool1):
    """
    对数据进行处理，从redis中读取数据，处理后，秒级数据放入OneSecond,；原始数据（高频数据）放入High
    :param pool: redis数据库
    :return:
    """
    #
    r = redis.StrictRedis(connection_pool=pool0)
    # 开关量跳变情况，保存在db=0中
    r1 = redis.StrictRedis(connection_pool=pool1)
    current_second = None
    data_type = "digital"
    """
    20180927
    增加one_second_data的格式
    one_second_data = {"DateTime":1, "Datas":{"300MT":1, "301MT":1}}
    """
    one_second_data = dict()  # 存储秒级数据

    """
    20180927
    增加high_data的格式
    high_data = {"300MT": {"DateTime": 1, "Frequency": 1, "Datas": [1, 2, 3, 4]},
                 "301MT": {"DateTime": 1, "Frequency": 1, "Datas": [1, 2, 3, 4]}}
    """
    high_data = dict()  # 存储高存数据
    data_list = []  # 存储1秒内的所有数据
    status_list = []  # 开关量状态
    index = 0
    while not QUITE:
        try:
            key = "CRIO:" + data_type
            data = r.brpop(key, timeout=1)
        except:
            continue
        if not data:
            continue
        else:
            real_datas = eval(data[1])

        # 对current_second和high_data进行初始化
        if not current_second:
            for key in real_datas['channel_names']:
                high_data[key] = {}
                high_data[key]["Datas"] = []

            current_second = real_datas['datetime']



        # 如果是同一秒的数据，就送入high_data
        if current_second == real_datas['datetime']:
            log = {}
            # 对于开关量，每个数据包中有数据组的个数不固定。data_list = [[1,2,3,3], [1,2,3,2], [3,2,1,2]]
            # 将每次的开关量求并集，如果开关量有边沿触发，将其送给redis。
            tmp_status_list = [False] * len(real_datas["channel_names"])
            for rd in real_datas['realdata']:
                for i in range(len(rd)):
                    status, edge, micro_time = digital_transform(rd[i])
                    tmp_status_list[i] = tmp_status_list[i] or status
                    if edge != 2:
                        tmp = {}
                        tmp["DateTime"] = current_second*1000 + micro_time
                        tmp["Status"] = edge
                        log[real_datas['channel_names'][i]] = tmp
                        r1.lpush("DigitalChange", log)
            status_list.append(tmp_status_list)


            index += 1
        else:
        # 如果不是同一秒的数据，对数据进行预处理，送入kafka和redis
            one_second_data['DateTime'] = current_second * 1000  # 精确到毫秒
            dd = {}
            date_time = current_second * 1000
            # 将秒级数据求平均，然后转换为字典{'4LHP300MT':300, '4LHP301MT':301}
            # 将布尔列表转换为二维整数数组,转置后，再转为列表
            # status_list = [[True, True, False], [False, True, False]]
            # tmp_status_list = [[1,0], [1,1], [0,0]]
            data_array = np.array(status_list).T  # 用于one_second_data计算
            tmp_status_list = np.array(status_list, dtype=int).T.tolist()  # 用于高存数据

            i = 0
            for key in real_datas['channel_names']:
                dd[key] = int(data_array[i].any())
                high_data[key] = {}
                high_data[key]["DateTime"] = date_time
                high_data[key]["Frequency"] = real_datas['frequency']
                high_data[key]["Datas"] = tmp_status_list
                i += 1
            one_second_data['Datas'] = dd

            # 将1秒的数据送入redis数据库
            try:
                key = "OneSecond:" + data_type
                r.lpush(key, one_second_data)
            except:
                continue

            # 将高存数据送往redis高速缓存
            try:
                key = "High:" + data_type + ":" + str(current_second)
                life_time = 50  # 生存时间
                r.setex(key, life_time, high_data)  # 将数据保存到redis中，生存时间50秒。时间到了之后，会自动删除。
            except:
                continue

            # 变量清空
            one_second_data.clear()  # 清空字典
            # 初始化
            current_second = real_datas['datetime']
            for rd in real_datas['realdata']:
                data_list.append(rd)

def data_relay_process(pool1, pool0):
    """
    对300CR继电器输出数据进行处理，从redis中读取数据，处理后，数据放入DB:relay,
    :param pool: redis数据库
    :return:
    """
    #
    r1 = redis.StrictRedis(connection_pool=pool1)
    r0 = redis.StrictRedis(connection_pool=pool0)

    current_second = None
    data_type = "relay"
    """
    20180927
    增加one_second_data的格式
    one_second_data = {"DateTime":1, "Datas":{"300MT":1, "301MT":1}}
    """
    one_second_data = dict()  # 存储秒级数据

    """
    20180927
    增加high_data的格式
    high_data = {"300MT": {"DateTime": 1, "Frequency": 1, "Datas": [1, 2, 3, 4]},
                 "301MT": {"DateTime": 1, "Frequency": 1, "Datas": [1, 2, 3, 4]}}
    """
    high_data = dict()  # 存储高存数据
    status_list = []  # 开关量状态


    while not QUITE:
        try:
            key = "CRIO:" + data_type
            data = r1.brpop(key, timeout=1)
        except:
            continue
        if not data:
            continue
        else:
            real_datas = eval(data[1])

        # 对current_second和high_data进行初始化
        if not current_second:
            for key in real_datas['channel_names']:
                high_data[key] = {}
                high_data[key]["Datas"] = []
            current_second = real_datas['datetime']

        # 数据处理
        tmp_status_list = [False] * len(real_datas["channel_names"])

        for rd in real_datas['realdata']:
            for i in range(len(rd)):
                status, edge, micro_time = digital_transform(rd[i])
                tmp_status_list[i] = tmp_status_list[i] or status
            # 对于数字量，1个数据包中可能包含多次数据
        status_list.append(tmp_status_list)

        if current_second != real_datas['datetime']:
            one_second_data['DateTime'] = current_second * 1000  # 精确到毫秒
            dd = {}
            date_time = current_second * 1000
            # 将秒级数据求平均，然后转换为字典{'4LHP300MT':300, '4LHP301MT':301}
            # 将布尔列表转换为二维整数数组,转置后，再转为列表
            # status_list = [[True, True, False], [False, True, False]]
            # tmp_status_list = [[1,0], [1,1], [0,0]]
            data_array = np.array(status_list).T  # 用于one_second_data计算
            tmp_status_list = np.array(status_list, dtype=int).T.tolist()  # 用于高存数据

            i = 0
            for key in real_datas['channel_names']:
                dd[key] = int(data_array[i].any())
                high_data[key] = {}
                high_data[key]["DateTime"] = date_time
                high_data[key]["Frequency"] = real_datas['frequency']
                high_data[key]["Datas"] = tmp_status_list[i]
                i += 1
            one_second_data['Datas'] = dd

            # 将1秒的数据送入redis数据库
            try:
                key = "OneSecond:" + data_type
                r1.lpush(key, one_second_data)
            except:
                continue

            # 将高存数据送往redis高速缓存
            try:
                key = "High:" + data_type + ":" + str(current_second)
                life_time = 50  # 生存时间
                r1.setex(key, life_time, high_data)  # 将数据保存到redis中，生存时间50秒。时间到了之后，会自动删除。
            except:
                continue

            # 变量清空
            one_second_data.clear()  # 清空字典
            status_list = []
            # 初始化
            current_second = real_datas['datetime']



# this is a demo



def kafka_producer(pool):
    # 将数据从redis中取出，以json格式放入kafka
    # 输入：q：队列
    broker = BROKER
    # topic：模拟量报警信息

    r1 = redis.StrictRedis(connection_pool=pool)
    print("r1", r1)
    # 配置Producer
    conf = {'bootstrap.servers': broker}
    while not QUITE:
        # 创建Producer实例
        try:
            p = Producer(**conf)
        except KafkaError as e:
            print("Producer: %s" % e)
            time.sleep(1)
            continue
        index = 0
        while True:
            try:
                try:
                    # 模拟量
                    # try:
                    #     _, one_second_analog = r1.brpop("OneSecond:analog", timeout=1)  # 从队列获取数据
                    # except:
                    #     continue
                    # if one_second_analog:
                    #     p.produce("CurrentAnalogData", one_second_analog)  # 发送到kafka

                    # 温度
                    try:
                        data = r1.brpop("OneSecond:temperature", timeout=1)  # 从redis获取数据
                    except:
                        continue
                    if data[1]:
                        p.produce("CurrentTemperatureData", data[1])  # 发送到kafka
                        print(data[1])


                    # 开关量
                    # try:
                    #     _, one_second_digital = r1.brpop("OneSecond:digital", timeout=1)  # 从redis获取数据
                    # except:
                    #     continue
                    # if not one_second_digital:
                    #     p.produce("CurrentDigitalData", one_second_digital)  # 发送到kafka


                    index += 1
                    print('发送的消息数量：', index)
                except BufferError as e:
                    print("Error Produce Buffer: %s" % e)  # 保存错误信息
                p.poll(0)
                p.flush()
            except KeyboardInterrupt:
                print('%% Aborted by user\n')

def kafka_consumer():
    # 从kafka获取实时数据，打印，供查看kafka中数据是否正确
    BROKER = 'dxg-ubuntu:9092'
    broker = BROKER
    # topics = settings.CMD_TOPICS
    INFO_TOPICS = ["CurrentTemperatureData"
                   #"CurrentAnalogData",
                   #"CurrentDigitalData"
                   ]

    topics = INFO_TOPICS
    c = Consumer({
        'bootstrap.servers': broker,
        'group.id': 'group1',
        'client.id': 'dxg',
        'default.topic.config': {
            'auto.offset.reset': 'smallest'
        }
    })

    c.subscribe(topics)

    try:
        while not QUITE:

                kafka_info = c.poll(timeout=1.0)  # 从kafka获取消息

                if kafka_info is None:
                    continue

                if kafka_info.error():
                    if kafka_info.error().code() == KafkaError._PARTITION_EOF:
                        pass
                        # print(('%% %s [%d] reached end at offset %d\n' %
                        #              (msg.topic(), msg.partition(), msg.offset())))
                    else:
                        raise KafkaException(kafka_info.error())
                if kafka_info.value() == b'Broker: No more messages':
                    continue

                print("topic:", kafka_info.topic(), "----", "Data:", kafka_info.value())

    except KeyboardInterrupt:
        print('%% Aborted by user\n')
    finally:
        # Close down consumer to commit final offsets.
        c.close()


def digital_transform(data):
    """
    将开关量的值，按照定义的格式进行转换，并返回转换结果
    :param data:
    :return:
    """

    # 将数值转换为字符串，字符串长度为6位。例如：10023转换为"010023"
    s = str(int(data)).rjust(6, "0")
    status = bool(int(s[0]))  # 开关量当前状态 1为高，0为低
    edge = int(s[1])  # 边沿变化方向 1为上升沿，0为下降沿，2表示没有变化
    micro_time = int(s[2:])  # 边沿发生的毫秒值
    return status, edge, micro_time



if __name__ == '__main__':
    ss = simulate_tcp_data()
    print(ss)



