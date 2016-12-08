#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@author:Eric.xin
"""
import os
import sys

reload(sys)
sys.setdefaultencoding("utf-8")
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.append(parentdir)

from Conf import s_config
import logging
from sqls import source_sql
import MySQLdb as mydb
import json
BASE_DIR = os.path.dirname(__file__)
os_path=os.path.join(BASE_DIR,'logs')

class S_db(object):
    def __init__(self,host,user,password,databases):
        self.__result={}
        self.__User_id = []
        self.__Available_amount = []
        self.__Freeze_amount = []
        self.__Ds_principal = []
        self.__Ds_interest = []
        self.__Reward = []
        try:
            self.host=host
            self.user=user
            self.password=password
            self.databases=databases

        #except Exception,e:
        except mydb.Error as e:
            print e,
            logging.basicConfig(level=logging.INFO, filename=os_path+"/"+'connet.log',
                                format='%(asctime)s  %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
            logging.info(u"数据库连接异常%s") %e

    def get_data(self):
        try:
            _ConnetDB=mydb.connect(self.host,self.user,self.password,self.databases) #连接数据库
            cursor=_ConnetDB.cursor() #游标
            cursor.execute(source_sql.ssql) #执行查询语句
            result_data=cursor.fetchall() #获取所有数据
            for data in result_data:
                self.__User_id.append(data[0])  # 用户id
                self.__Available_amount.append(data[1])  # 可用余额
                self.__Freeze_amount.append(data[2])  # 冻结金额
                self.__Ds_principal.append(data[3])  # 待收本金
                self.__Ds_interest.append(data[4])  # 待收收益
                self.__Reward.append(data[5])  # 奖励金额

                self.__result['user_id'] = self.__User_id
                self.__result['available_amount'] = self.__Available_amount
                self.__result['freeze_amount'] = self.__Freeze_amount
                self.__result['ds_principal'] = self.__Ds_principal
                self.__result['ds_interest'] = self.__Ds_interest
                self.__result['reward'] = self.__Reward
            return self.__result

        except Exception as e:
            logging.basicConfig(level=logging.INFO, filename='error.log',
                                format='%(asctime)s  %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
            logging.info("网络异常无法连接到数据库:%s"%e)









if __name__=="__main__":
    pass
    """
     s = S_db(s_config.ip, s_config.user, s_config.pwd, s_config.db)
     print s.get_data()
    """


















