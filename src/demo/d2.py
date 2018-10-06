#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

import time
import pymongo
import threading

def a():
    for i in range(10):
        time.sleep(1)
        print("a", i)

t1 = threading.Thread(target=a)

for i in range(3):
    print(i)
    time.sleep(1)
t1.start()
for i in range(3):
    print("b", i)
    time.sleep(1)


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

        # 如果是同一秒的数据，就送入high_data
        if current_second == real_datas['datetime']:

            # 对于模拟量，每个数据包中有500组数据。data_list = [[1,2,3,3], [1,2,3,2], [3,2,1,2]]
            for rd in real_datas['realdata']:
                data_list.append(rd)

            index += 1
        else:
        # 如果不是同一秒的数据，对数据进行预处理，送入kafka和redis
            one_second_data['DateTime'] = current_second * 1000  # 精确到毫秒
            dd = {}
            date_time = current_second * 1000
            # 将秒级数据求平均，然后转换为字典{'4LHP300MT':300, '4LHP301MT':301}

            data_array = np.array(data_list)  # 将列表转换为二维数组
            data_mean = np.mean(data_array, axis=0).tolist()  # 对二维列表求均值，再转为列表
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
                r.setex(key, high_data, life_time)  # 将数据保存到redis中，生存时间50秒。时间到了之后，会自动删除。
            except:
                continue

            # 变量清空
            one_second_data.clear()  # 清空字典
            # 初始化
            current_second = real_datas['datetime']
            data_list.append(real_datas['realdata'])





