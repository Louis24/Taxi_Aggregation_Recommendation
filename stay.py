#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  @Time    : 2021/2/23 17:57
#  @Author  : Louis Li
#  @Email   : vortex750@hotmail.com


import sys
import itertools
import numpy as np
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
from polygon import isPoiWithinPoly
from utils import v_mercator
from kafka import Message

pd.options.mode.chained_assignment = None
pd.set_option("display.max_columns", None)


class MySQL:
    def __init__(self, Date, T1, T2):
        self.user = "tmkj"
        self.password = "Tmkj@taxi123"
        # self.host = "192.168.245.34" # 郑州
        self.host = "192.168.247.205"  # 漯河
        self.database = "gps"
        self.cnx = mysql.connector.connect(user=self.user,
                                           password=self.password,
                                           host=self.host,
                                           database=self.database)
        self.Date = Date  # 起始日期（YMD）[str] 2021-03-05
        self.T1 = T1  # 起始时间（H）[int] 12
        self.T2 = T2  # 终止小时（H）[int] 14

    def t_taxi_realtime_info(self):
        x = self.Date.replace("-", "")
        p = f"{self.Date} {self.T1}:00:00"
        q = f"{self.Date} {self.T2}:00:00"

        sql = f"SELECT vehicle_no, site_time, lng, lat, speed from t_taxi_realtime_info_{x} where site_time < '{q}' and site_time >= '{p}'"
        print(sql)
        df = pd.read_sql(sql, self.cnx)
        df.columns = ["vehicleNo", "time", "lng", "lat", "speed"]
        df.dropna(axis=0, how='any', inplace=True)
        df = df[df["lng"] != 0]  # 去除GIS错误
        df.to_csv('t_taxi_realtime_info.csv', encoding='ANSI', index=False)


class Stay:
    def __init__(self, n, t, uid, zone):
        self.n = n  # 每分钟6个点
        self.t = t  # 在区域内停留分钟数
        self.uid = uid  # 标识码
        self.zone = zone  # 区域格式list of list [lng,lat]

    def calculate(self):
        data = pd.read_csv("t_taxi_realtime_info.csv", encoding='ANSI')

        # MERCATOR
        zone = pd.DataFrame(self.zone, columns=['LNG', 'LAT'])
        lng = zone["LNG"]
        lat = zone["LAT"]
        x, y = v_mercator(lng, lat)
        vertices = [list(zip(x, y))]

        lng = data["lng"]
        lat = data["lat"]
        x, y = v_mercator(lng, lat)
        data["X"] = x
        data["Y"] = y

        # VEHICLE
        res = []
        grouped = data.groupby(["vehicleNo"])

        for i in grouped:
            car = i[0]
            series = i[1]
            points = list(zip(series["X"], series["Y"]))

            IN = []
            for point in points:
                w = isPoiWithinPoly(point, vertices)
                IN.append(w)

            if sum(IN) < self.t * self.n:
                continue

            else:
                Time = series["time"].tolist()

                # p = [list(j) for i, j in itertools.groupby(IN)]  # 原序列
                # print("ORIGINAL SEQUENCE:\n", p)

                q1 = [len(list(j)) if i == 1 else -1 for i, j in itertools.groupby(IN)]  # 长度序列
                q2 = [len(list(j)) for _, j in itertools.groupby(IN)]  # 长度序列
                # print("LENGTH SEQUENCE:\n", q1)
                # print("LENGTH SEQUENCE:\n", q2)

                r = int(np.argmax(np.array(q1)))  # 最大长度索引
                s = max(q1)  # 最大长度
                # print("MAX LENGTH INDEX AND MAX LENGTH:\n", r, s)

                l2 = sum(q2[:r + 1])
                l1 = l2 - s
                # print("LOCATION:\n", l1, l2)

                t1 = datetime.strptime(Time[l1], "%Y-%m-%d %H:%M:%S")
                t2 = datetime.strptime(Time[l2 - 1], "%Y-%m-%d %H:%M:%S")

                t3 = timedelta(minutes=self.t)

                if ((t2 - t1) >= t3):
                    length = str(t2 - t1)
                    res.append([car, length, self.uid])

        print("RESULT:\n", res)

        res = pd.DataFrame(res, columns=['VEHICLE', 'DURATION_MINUTE', "ID"])
        res.to_csv('t_taxi_realtime_info_stay.csv', encoding='ANSI', index=False, float_format='%.2f')

        y = []

        for i in res.itertuples():
            x = dict()
            x["vehicle"] = i[1]
            x["duration_minute"] = i[2]
            x["uid"] = i[3]
            y.append(x)

        # with open('stay.json', 'w') as f:
        #     json.dump(y, f)

        hosts = "192.168.245.34:2181,192.168.245.35:2181,192.168.245.36:9092"
        topic = "taxi_stay"
        message = Message(hosts=hosts, topic=topic)

        msg = str(y)
        print(msg)
        message.send(msg)


def main():
    x = MySQL(Date=sys.argv[1], T1=sys.argv[2], T2=sys.argv[3])
    x.t_taxi_realtime_info()

    y = Stay(n=eval(sys.argv[4]), t=eval(sys.argv[5]), uid=sys.argv[6], zone=eval(sys.argv[7]))
    y.calculate()


if __name__ == "__main__":
    main()
