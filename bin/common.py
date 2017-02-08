#!/usr/bin/env python
# encoding: utf-8

import os
import sys
reload(sys)

top = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(top)


def get_all_data_from_source():
    def __get_data():
        from Conf import s_config
        sql = "select user_id,available_amount/100, freeze_amount/100, ds_principal/100, ds_interest/100, reward/100" \
              " from jr_pay.pay_account"
        conn = s_config.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        all_data = cursor.fetchall()
        conn.close()

        dict_list = list()
        for _data in all_data:
            _dict = dict()
            _dict["user_id"] = _data[0]
            _dict["WeiChuJieHu"] = _data[1]
            _dict["TiXianHu"] = _data[2]
            _dict["DaiFuBenJinHu"] = _data[3]
            _dict["DaiFuLiXiHu"] = _data[4]
            _dict["YingXiaoHu"] = _data[5]
            dict_list.append(_dict)
        return dict_list

    dict_data = dict()
    for _record in __get_data():
        _record = dict(_record)
        user_id = str(_record["user_id"])
        _record.pop("user_id")
        dict_data[user_id] = _record

    return dict_data


def get_all_data_from_destination():
    def __get_data():
        from Conf import d_config
        sql = "select a.user_id, b.ledger_code, b.ledger_tag, b.amount" \
              " from customer_business_entity a LEFT JOIN ledgers_entity b ON a.id = b.customer_business_id"

        conn = d_config.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        all_data = cursor.fetchall()
        conn.close()

        dict_list = list()
        for data in all_data:
            _dict = dict()
            _dict["user_id"] = data[0]
            _dict["ledger_code"] = data[1]
            _dict["ledger_tag"] = data[2]
            _dict["amount"] = data[3]
            dict_list.append(_dict)
        return dict_list

    dict_data = dict()
    for _record in __get_data():
        _record = dict(_record)
        user_id = str(_record["user_id"])
        if user_id not in dict_data:
            dict_data[user_id] = dict()
        dict_data[user_id][_record["ledger_code"]] = _record["amount"]

    return dict_data


def print_dict(dict_data, num=None):
    count = 1
    for _key_, _value_ in dict_data.items():
        print _key_, _value_
        count += 1
        if not num:
            continue
        elif count > num:
            break
        else:
            pass
