#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  @Time    : 2021/3/25 17:14
#  @Author  : Louis Li
#  @Email   : vortex750@hotmail.com


import sys
import numpy as np
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
from utils import v_mercator, v_geodetic
from sklearn.cluster import DBSCAN
from calipers import diameter
from kafka import Message
from hull import hull

pd.options.mode.chained_assignment = None
pd.set_option("display.max_columns", None)


class MySQL:
    def __init__(self):
        self.user = "tmkj"
        self.password = "Tmkj@taxi123"
        # self.host = "192.168.245.34" # 郑州
        self.host = "192.168.247.205"  # 漯河
        self.database = "gps"
        self.cnx = mysql.connector.connect(user=self.user,
                                           password=self.password,
                                           host=self.host,
                                           database=self.database)

    def t_taxi_realtime_info(self):
        x = datetime.now().date().strftime("%Y%m%d")
        y = datetime.now() - timedelta(minutes=10)  # 时间运算
        z = y.strftime("%Y-%m-%d %H:%M:%S")

        sql = f"SELECT vehicle_no, site_time, lng, lat, speed from t_taxi_realtime_info_{x} where site_time > '{z}'"
        df = pd.read_sql(sql, self.cnx)
        df.columns = ["vehicleNo", "time", "lng", "lat", "speed"]
        df.dropna(axis=0, how='any', inplace=True)
        df = df[df["lng"] != 0]  # 去除GIS错误
        df.to_csv('t_taxi_realtime_info.csv', encoding='GBK', index=False)


class Gather:
    def __init__(self, speed_limit=3, eps=300 * 1.2202, min_samples=10, min_size=1000, max_size=5000):
        self.speed_limit = speed_limit  # 判断聚集的车辆速度限制
        self.eps = eps  # 当前这个点的搜索半径
        self.min_samples = min_samples  # 当前这个点的搜索半径内有几个点
        self.min_size = min_size  # 获取凸包的最小直径
        self.max_size = max_size  # 获取凸包的最大直径

    def calculate(self):
        t_taxi_realtime_info = pd.read_csv("t_taxi_realtime_info.csv", encoding="GBK")
        t_taxi_realtime_info = t_taxi_realtime_info[t_taxi_realtime_info["speed"] <= self.speed_limit]

        grouped = t_taxi_realtime_info.groupby(["vehicleNo"])
        res = []

        for j in grouped:
            vehicle = j[0]
            series = j[1]
            lng = series["lng"].mean()
            lat = series["lat"].mean()

            res.append([vehicle, lng, lat])

        res = pd.DataFrame(res, columns=['vehicleNo', 'lng', 'lat'])

        if len(res) == 0:
            print("Missing Data")
            return 0

        lng = res["lng"]
        lat = res["lat"]

        x, y = v_mercator(lng, lat)
        p = [list(i) for i in zip(x, y)]
        p = np.array(p)  # each sample's xy

        q = DBSCAN(eps=self.eps, min_samples=self.min_samples, metric="euclidean").fit_predict(
            p)  # -1=single points @numpy.ndarray

        res["CLUSTER"] = q
        res.to_csv('t_taxi_realtime_info_gather.csv', encoding='GBK', index=False, float_format='%.6f')

        t_taxi_realtime_info_gather = res[res["CLUSTER"] != -1]

        grouped = t_taxi_realtime_info_gather.groupby(["CLUSTER"])
        res = []

        for i in grouped:
            series = i[1]

            v = dict()

            shape = []
            vehicles = []

            if len(series) == 1:
                continue

            else:
                lng = series["lng"]
                lat = series["lat"]

                x, y = v_mercator(lng, lat)

                x = [int(j) for j in x]  # 精度问题 cv2无法处理浮点
                y = [int(j) for j in y]

                p = list(zip(x, y))
                h = hull(p)
                # print(h) # 测试序列I

                if self.min_size <= diameter(h) <= self.max_size:  # add this condition may cause
                    """ Obviously np did not create a copy when rotatingCalipers was running """
                    # print(h) # 测试序列II
                    x = [j[1] for j in h]  # hull中的计算结果相反
                    y = [j[0] for j in h]

                    lng, lat = v_geodetic(x, y)

                    for j in range(len(lng)):
                        u = dict()
                        u["lng"] = lng[j]
                        u["lat"] = lat[j]
                        shape.append(u)

                    for j in series.itertuples():
                        u = dict()
                        u["vehicleNo"] = j[1]
                        u["lng"] = j[2]
                        u["lat"] = j[3]
                        vehicles.append(u)

                    v["shape"] = shape
                    v["vehicles"] = vehicles
                    res.append(v)

        # 测试
        # res = f"{res}"
        # res = res.replace("'", '"')
        # with open(f'gather eps={int(self.eps / 1.2202)} min_samples={self.min_samples}.json', 'w') as f:
        #     f.write(res)

        # 运行
        # res = f"{res}"
        # res = res.replace("'", '"')
        # with open(f'gather.json', 'w',encoding="UTF-8") as f:
        #     f.write(res)
        # json.dump(res, f)

        hosts = "192.168.245.34:2181,192.168.245.35:2181,192.168.245.36:9092"
        topic = "taxi_gather"
        message = Message(hosts=hosts, topic=topic)

        msg = str(res)
        print(msg)
        message.send(msg)


def parameter():
    for eps in [100, 200, 300, 500]:

        for min_samples in [1, 2, 5, 10]:
            x = Gather(eps=eps * 1.2202, min_samples=min_samples)
            x.calculate()


def main():
    x = MySQL()
    x.t_taxi_realtime_info()

    y = Gather(speed_limit=eval(sys.argv[1]), eps=eval(sys.argv[2]), min_samples=eval(sys.argv[3]),
               min_size=eval(sys.argv[4]), max_size=eval(sys.argv[5]))
    y.calculate()

    # parameter()


if __name__ == '__main__':
    main()
