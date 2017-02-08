#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@author:Eric.xin
"""

import MySQLdb as MySQL

ip = '172.16.6.11'
user = 'root'
pwd = 'my-secret-pw'
db = 'jr_pay'


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

