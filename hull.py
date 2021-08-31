#!/usr/bin/python
import cv2
import numpy as np


def hull(points):
    points = np.array(points)
    res = cv2.convexHull(points, clockwise=True, returnPoints=True)
    res = np.squeeze(res)
    return res


def main():
    x = hull([[0, 0], [4, 0], [2, 2], [1, 1]])
    print(x)


if __name__ == '__main__':
    main()
