#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

import utils
import time
import datetime
import numpy as np
import pandas as pd
import redis

IS_INIT = True


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

def data_process(q1, q2, q3):
    """
    对数据进行处理，从q1中读取数据，处理后，秒级数据放入q2,；原始数据（高频数据）放入redis
    :param q1: 队列；存储从tcpserver读取的dict类型原始数据
    :param q2: 队列；存储秒级dict类型数据
    :param q3：队列；存储高频dict类型数据
    :return:
    """
    #  温度的高存数据保存在db=1中，模拟量保存在db=2中，开关量保存在db=3中
    r = redis.Redis(host='localhost', port=6379, db=1)
    current_second = None
    df = pd.DataFrame()
    """
    20180927
    增加current_temperature_data的格式
    current_temperature_data = {"DateTime":1, "Datas":{"300MT":1, "301MT":1}}
    """

    current_temperature_data = dict()  # 存储放入kafka的数据
    """
    20180927
    增加high_data的格式
    high_data = {"300MT": {"DateTime": 1, "Frequency": 1, "Datas": [1, 2, 3, 4]},
                 "301MT": {"DateTime": 1, "Frequency": 1, "Datas": [1, 2, 3, 4]}}
    """

    high_data = dict() #存储放入MongoDB的高频数据
    index = 0
    while True:
        try:
            real_datas = q1.get(block=False)
            q1.task_done()
            print("time", real_datas['datetime'])
        except:
            continue

        # 对current_second和high_data进行初始化
        if not current_second:
            for key in real_datas['channel_names']:
                high_data[key] = {}
                high_data[key]["Datas"] = []

            current_second = real_datas['datetime']
            # df = pd.DataFrame(columns=real_datas['channel_names'])

        # 如果是同一秒的数据，就送入high_data
        if current_second == real_datas['datetime']:
            i = 0
            for key in real_datas['channel_names']:
                high_data[key]["Datas"].append(real_datas['realdata'][i])
                high_data[key]["DateTime"] = current_second * 1000
                high_data[key]["Frequency"] = real_datas['frequency']
                i += 1
            # df.loc[index] = real_datas['realdata']
            index += 1
        else:
        # 如果不是同一秒的数据，对数据进行预处理，送入kafka和redis
            current_temperature_data['DateTime'] = current_second * 1000  # 精确到毫秒
            dd = {}
            # dd = df.mean().to_dict()  # 将秒级数据求平均，然后转换为字典{'4LHP300MT':300, '4LHP301MT':301}
            for key in high_data:
                dd[key] = high_data[key]["Datas"]

            current_temperature_data['Datas'] = dd
            # 将数据放入队列，队列将发给kafka
            try:
                q2.put(current_temperature_data, block=True)
                q2.task_done()
                # print(current_temperature_data)
            except:
                print("q2 error")
                continue
            print("队列大小：", q2.qsize())

            # 将1秒的数据送入redis数据库
            try:
                r.lpush("current_temperature_data", current_temperature_data)
            except:
                continue

            # 将高存数据送往redis高速缓存
            try:
                name = str(current_second)
                life_time = 50  # 生存时间
                r.setex(name, high_data, life_time)  # 将数据保存到redis中，生存时间50秒。时间到了之后，会自动删除。
            except:
                continue


            # 变量清空
            # df.drop(df.index, inplace=True)  # 清空df
            current_temperature_data.clear()  # 清空字典

            # 初始化
            current_second = real_datas['datetime']
            i = 0
            for key in real_datas['channel_names']:
                high_data[key]["Datas"] = []
                high_data[key]["Datas"].append(real_datas['realdata'][i])
                high_data[key]["DateTime"] = current_second * 1000
                high_data[key]["Frequency"] = real_datas['frequency']
                i += 1
            index = 1  # 初始化索引

            # current_second = real_datas['datetime']  # 将下一秒赋给current_second

            # df.loc[index] = real_datas['realdata']  #


if __name__ == '__main__':

    # real_datas = get_realdata()
    # run(real_datas)
    pass
