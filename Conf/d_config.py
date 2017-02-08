#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@author:Eric.xin
"""
import MySQLdb as MySQL

ip = "172.16.5.3"
user = 'acc_plat_t'
pwd = 'Zh83Tes*tU3'
db = 'acc_plat'


def get_connection():
    try:
        conn = MySQL.connect(ip, user, pwd, db, charset='utf8')
        print "SUCCESS"
        return conn
    except BaseException, e:
        print str(e)
    return None

if __name__ == "__main__":
    connection = get_connection()
    if connection:
        connection.close()

