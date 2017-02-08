#!/usr/bin/env python
# encoding: utf-8
"""
@author:Eric.xin
"""

import logging
import multiprocessing
import os
import sys
import time

reload(sys)

top = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(top)

from Conf import d_config as db_destination
from Conf import s_config as db_source
from bin.source_data import SourceDB


LOG_INFO = os.path.join(top, "logs", "update_info.log")
LOG_ERROR = os.path.join(top, "logs", "update_error.log")


def get_all_data():
    """
    实例化SourceDB类,通过get_data方法获取查询的数据值
    :return:
    """
    source = SourceDB(db_source.ip, db_source.user, db_source.pwd, db_source.db)
    origin_data = dict(source.get_data())
    del source

    for _key in origin_data:
        print _key, len(origin_data[_key])
    return origin_data


class DestinationDB(object):
    """
    定义基础类,用与连接数据库
    """
    def __init__(self, host, user, password, databases):

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

    @staticmethod
    def db_update(sql_list):
        """
        连接数据库
        循环执行sql
        关闭数据库
        :param sql_list:
        :return:
        """
        print len(sql_list)
        print "==== ===="
        # 配置logging选项
        logging.basicConfig(level=logging.INFO, filename=LOG_INFO, format='%(asctime)s  %(message)s')
        logging.basicConfig(level=logging.ERROR, filename=LOG_ERROR, format='%(asctime)s  %(message)s')

        conn = db_destination.get_connection()
        if not conn:
            return

        # 程序开始
        count = 0
        cursor = conn.cursor()
        sql_list = list(sql_list)
        total_count = len(sql_list)

        start_time = time.strftime('%Y.%m.%d_%H.%M.%S')
        # 循环执行SQL语句
        for sql in sql_list:
            count += 1
            msg = " (%s / %s) " % (str(count), str(total_count))
            logging.info(msg=msg)
            logging.info(msg=sql)
            print msg

            try:
                # cursor.execute("set autocommit=off")
                cursor.execute(sql)
            except BaseException as e:
                logging.error(sql)
                logging.error(str(e))
            finally:
                pass

            if not count % 2000:
                conn.commit()
        # cursor.execute("set autocommit=on")
        conn.commit()
        cursor.close()
        conn.close()
        print "START : " + start_time
        print "ENDED : " + time.strftime('%Y.%m.%d_%H.%M.%S')
        print "===============================================\n"


def update_users(sql_list):
    DestinationDB.db_update(sql_list=sql_list)


def multiprocess_update(sql_list):
    """
    创建进程并返回进程对象实例

    :param sql_list:
    :return:
    """
    return multiprocessing.Process(target=update_users, args=(sql_list,))


def _run_multi(list_of_sql_list):
    """
    根据列表长度创建对应的子进程做并发操作

    :param list_of_sql_list:
    :return:
    """
    print len(list_of_sql_list)
    print "======================"
    for sql_list in list_of_sql_list:
        multiprocess_update(sql_list).start()

    while True:
        time.sleep(10)


def _split_list(total_count, group_size):
    """
    根据列表长度和每个分组的数量返回列表
    [(x1, y1), (x2, y2), (x3, y3), ...]

    :param total_count: 需要切片的列表长度
    :param group_size: 每个分组的长度
    :return:
    """
    num = total_count / group_size
    list_groups = list()

    for index in range(num):
        start = index * group_size
        end = start + group_size
        list_groups.append((start, end))

    last = total_count % group_size
    if last:
        start = num * group_size
        end = start + last
        list_groups.append((start, end))

    return list_groups


def generate_all_sql():
    """
    根据来自源数据库的数据 生成SQL 列表
    :return: sql list
    """
    all_data = get_all_data()
    all_data = dict(all_data)

    sql_list = list()

    user_id = all_data["user_id"]
    total = len(user_id)
    print total

    ledger_map = {"available_amount": "WeiChuJieHu",
                  "ds_principal": "DaiFuBenJinHu",
                  "ds_interest": "DaiFuLiXiHu",
                  "reward": "YingXiaoHu",
                  "freeze_amount": "TiXianHu"}

    sql_common = "update ledgers_entity,customer_business_entity" \
                 " set ledgers_entity.amount=%s where" \
                 " ledgers_entity.customer_business_id=customer_business_entity.id" \
                 " and customer_business_entity.user_id='%s'" \
                 " and ledgers_entity.ledger_code='%s'" \
                 " and customer_business_entity.application_id='YGONLINE'"

    for index in xrange(total):
        for _type in ("available_amount", "ds_principal", "ds_interest", "reward", "freeze_amount"):
            sql_list.append(sql_common % (all_data[_type][index], all_data["user_id"][index], ledger_map[_type]))

    return sql_list


if __name__ == "__main__":
    if os.path.exists(LOG_INFO):
        os.remove(LOG_INFO)
    if os.path.exists(LOG_ERROR):
        os.remove(LOG_ERROR)

    all_sql = generate_all_sql()
    print len(all_sql)
    print

    print "\n".join([str(sql) for sql in all_sql[:5]])

    groups = _split_list(total_count=len(all_sql), group_size=8000)
    user_group_list = list()
    for _x, _y in groups:
        user_group_list.append(all_sql[_x:_y])

    _run_multi(user_group_list)
