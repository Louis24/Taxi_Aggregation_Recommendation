#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  @Time    : 2021/3/9 9:17
#  @Author  : Louis Li
#  @Email   : vortex750@hotmail.com


import phoenixdb.cursor

database_url = 'http://192.168.58.58:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)

Date = "2021-03-05"
T1 = 12
T2 = 14

p = f"{Date} {T1}:00:00"
q = f"{Date} {T2}:00:00"

cursor = conn.cursor()
sql = f'SELECT "vehicle_no", "site_time", "lng", "lat", "speed" from TAXI_LOCATION ' \
      f'where "site_time" < \'{q}\' and "site_time" >= \'{p}\''

print(sql)
cursor.execute(sql)
print(cursor.fetchall())

"""
OSError: Could not find KfW installation. Please download and install the 64bit Kerberos for Windows 
MSI from https://web.mit.edu/KERBEROS/dist and ensure the 'bin' folder (C:\Program Files\MIT\Kerberos\bin)
is in your PATH.
"""
