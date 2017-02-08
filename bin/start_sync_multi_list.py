#!/usr/bin/env python
# encoding: utf-8
"""
@author:Eric.xin
"""

import os
import sys
import time
import multiprocessing
import logging
import MySQLdb as MySQL
reload(sys)
top = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(top)
from Conf import d_config
from Conf import s_config
from bin.source_data import SourceDB

LOG_INFO = os.path.join(top, "logs", "update_info.log")
LOG_ERROR = os.path.join(top, "logs", "update_error.log")


def get_all_data():
    """
    获取源数据库的数据
    :return: {key1:[], key2:[], ...}
    """
    source = SourceDB(s_config.ip, s_config.user, s_config.pwd, s_config.db)

    origin_data = dict(source.get_data())
    all_len = list()
    for _key in origin_data:
        all_len.append(len(origin_data[_key]))
        print _key, len(origin_data[_key])

    return origin_data


class DestinationDB(object):
    """
    定义基础类,用与连接数据库
    """
    def __init__(self, host, user, password, databases):

        self.__Data = get_all_data()
        try:
            self.host = host
            self.user = user
            self.password = password
            self.databases = databases
        except Exception, e:
            print e,
            logging.basicConfig(level=logging.INFO, filename='dest_connet.log',
                                format='%(asctime)s  %(message)s')
            logging.info(u"未连接到更新数据库")

    def up_wei_chu_jie(self):
        connect = MySQL.connect(self.host, self.user, self.password, self.databases)
        self.db_update(connection=connect, data_list=self.__Data['available_amount'], tag="WeiChuJieHu")
        connect.close()

    def up_dai_fu_ben_jin(self):
        connect = MySQL.connect(self.host, self.user, self.password, self.databases)
        self.db_update(connection=connect, data_list=self.__Data['ds_principal'], tag="DaiFuBenJinHu")
        connect.close()

    def up_dai_fu_li_xi(self):
        connect = MySQL.connect(self.host, self.user, self.password, self.databases)
        self.db_update(connection=connect, data_list=self.__Data['ds_interest'], tag="DaiFuLiXiHu")
        connect.close()

    def up_ying_xiao(self):
        connect = MySQL.connect(self.host, self.user, self.password, self.databases)
        self.db_update(connection=connect, data_list=self.__Data['reward'], tag="YingXiaoHu")
        connect.close()

    def up_ti_xian(self):
        connect = MySQL.connect(self.host, self.user, self.password, self.databases)
        self.db_update(connection=connect, data_list=self.__Data['freeze_amount'], tag="TiXianHu")
        connect.close()

    def db_update(self, connection, data_list, tag):
        start_time = time.strftime('%Y.%m.%d_%H.%M.%S')
        # 配置logging选项
        logging.basicConfig(level=logging.INFO, filename=LOG_INFO, format='%(asctime)s  %(message)s')
        logging.basicConfig(level=logging.ERROR, filename=LOG_ERROR, format='%(asctime)s  %(message)s')

        count = 0
        cursor = connection.cursor()
        data_list = list(data_list)
        total_count = len(data_list)

        for k in xrange(total_count):
            count += 1
            print " (%s / %s)  :  %s" % (str(count), str(total_count), tag)
            amount = data_list[k]

            user_id = self.__Data['user_id'][k]

            sql_common = "update ledgers_entity,customer_business_entity" \
                         " set ledgers_entity.amount=%s where" \
                         " ledgers_entity.customer_business_id=customer_business_entity.id" \
                         " and customer_business_entity.user_id='%s'" \
                         " and ledgers_entity.ledger_code='%s'" \
                         " and customer_business_entity.application_id='YGONLINE'"

            sql = sql_common % (float(amount), str(user_id), tag)

            try:
                logging.info(u"%s: %s user_id:%s sql:%s %s / %s " %
                             (tag, amount, user_id, sql, str(count), str(total_count)))
                cursor.execute("set autocommit=0")
                cursor.execute(sql)

            except BaseException as e:
                logging.basicConfig(level=logging.ERROR, filename='erro.log', format='%(asctime)s  %(message)s')
                logging.error(u"%s:%s" % tag, str(e))
                logging.error(u'%s:%s user_id:%s sql:%s' % (tag, str(amount), user_id, sql))
            finally:
                pass

            if not count % 2000:
                connection.commit()
            # break
        cursor.execute("set autocommit=1")
        connection.commit()
        cursor.close()
        print "START : " + start_time
        print "ENDED : " + time.strftime('%Y.%m.%d_%H.%M.%S')
        print "===============================================\n"


def _update_up_wcjh():
    d = DestinationDB(d_config.ip, d_config.user, d_config.pwd, d_config.db)
    d.up_wei_chu_jie()


def _update_up_dfbjh():
    d = DestinationDB(d_config.ip, d_config.user, d_config.pwd, d_config.db)
    d.up_dai_fu_ben_jin()


def _update_up_dflxh():
    d = DestinationDB(d_config.ip, d_config.user, d_config.pwd, d_config.db)
    d.up_dai_fu_li_xi()


def _update_up_txh():
    d = DestinationDB(d_config.ip, d_config.user, d_config.pwd, d_config.db)
    d.up_ti_xian()


def _update_up_yxh():
    d = DestinationDB(d_config.ip, d_config.user, d_config.pwd, d_config.db)
    d.up_ying_xiao()


def multiprocess_update(func):
    return multiprocessing.Process(target=func)


def _run_multi():
    processes = list()
    for func in [_update_up_wcjh, _update_up_dfbjh, _update_up_dflxh, _update_up_txh, _update_up_yxh]:
        processes.append(multiprocess_update(func))

    for _process in processes:
        _process.start()
        time.sleep(5)

    while True:
        time.sleep(10)

if __name__ == "__main__":
    _run_multi()
