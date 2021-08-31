#!/usr/bin/python

import numpy as np


def orientation(p, q, r):
    """Return positive if p-q-r are clockwise, neg if ccw, zero if colinear."""
    return (q[1] - p[1]) * (r[0] - p[0]) - (q[0] - p[0]) * (r[1] - p[1])


def hulls(points):
    """Graham scan to find upper and lower convex hulls of a set of 2d points."""
    u = []
    l = []
    points.sort()
    for p in points:
        while len(u) > 1 and orientation(u[-2], u[-1], p) <= 0: u.pop()
        while len(l) > 1 and orientation(l[-2], l[-1], p) >= 0: l.pop()
        u.append(p)
        l.append(p)
    return u, l


def rotatingCalipers(points):
    """Given a list of 2d points, finds all ways of sandwiching the points
        between two parallel lines that touch one point each, and yields the sequence
        of pairs of points touched by each pair of lines."""
    u, l = hulls(points)
    i = 0
    j = len(l) - 1
    while i < len(u) - 1 or j > 0:
        yield u[i], l[j]

        # if all the way through one side of hull, advance the other side
        if i == len(u) - 1:
            j -= 1
        elif j == 0:
            i += 1

        # still points left on both lists, compare slopes of next hull edges
        # being careful to avoid divide-by-zero in slope calculation
        elif (u[i + 1][1] - u[i][1]) * (l[j][0] - l[j - 1][0]) > \
                (l[j][1] - l[j - 1][1]) * (u[i + 1][0] - u[i][0]):
            i += 1
        else:
            j -= 1


def diameter(points):
    """Given a list of 2d points, returns the pair that"s farthest apart."""
    diam, pair = max([((p[0] - q[0]) ** 2 + (p[1] - q[1]) ** 2, (p, q))
                      for p, q in rotatingCalipers(points)])

    p1 = np.array(pair[0])
    p2 = np.array(pair[1])
    dist = np.linalg.norm(p1 - p2)

    return dist


def main():
    points = [(0, 0), (2, 0), (4, 0), (3, 2), (4, 4), (2, 4), (0, 2), (2, 2)]
    res = diameter(points)
    print(res)


if __name__ == '__main__':
    main()
