#!/usr/bin/python
import numpy as np
from math import sin, cos, tan, asin, radians, sqrt, log, pi, atan, exp

a = 6378245  # 长半轴
ee = 0.00669342162296594323  # 偏心率平方


def geodistance(lng1, lat1, lng2, lat2):
    """ return km """

    lng1, lat1, lng2, lat2 = map(radians, [lng1, lat1, lng2, lat2])
    dlon = lng2 - lng1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    distance = 2 * asin(sqrt(a)) * 6371
    distance = round(distance, 3)  # unit = km

    return distance


def geodetic2mercator(lng, lat):
    """ 距离有误差 """

    x = lng * 20037508.34 / 180
    y = log(tan((90 + lat) * pi / 360)) / (pi / 180)
    y = y * 20037508.34 / 180  # unit = m

    return x, y


def mercator2geodetic(x, y):
    """ 墨卡托逆变换 """

    lng = x / 20037508.34 * 180
    lat = y / 20037508.34 * 180
    lat = 180 / pi * (2 * atan(exp(lat * pi / 180)) - pi / 2)

    return lng, lat


def euclidean(x1, y1, x2, y2):
    vector = np.array([x1 - x2, y1 - y2])
    distance = np.linalg.norm(vector)

    return distance


v_distance = np.vectorize(geodistance)
v_mercator = np.vectorize(geodetic2mercator)
v_geodetic = np.vectorize(mercator2geodetic)
v_euclidean = np.vectorize(euclidean)


def main():
    x, y = geodetic2mercator(113.630188, 34.721301)
    print(x, y)
    lng, lat = mercator2geodetic(12649254, 4126071)
    print(lng, lat)

    x = euclidean(1, 0, 2, 1)
    print(x)


if __name__ == '__main__':
    main()
