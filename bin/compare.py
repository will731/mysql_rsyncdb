#!/usr/bin/env python
# encoding: utf-8
"""
@author:Eric.xin
"""

import os
import sys
import decimal
import time
reload(sys)

top = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(top)

from common import get_all_data_from_source


def parse_record_file(file_name):
    """
    解析csv文件 由DBA协助导出

    数据列分别为：
        0. user_id
        1. ledger_code
        2. ledger_tag
        3. amount

    :param file_name:
    :return:
    """
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), file_name)
    record = dict()

    with open(path) as records:
        for line in records:
            if line:
                line = line.strip()
                items = line.split(',')
                user_id = str(items[0])
                if user_id not in record:
                    record[user_id] = dict()
                record[user_id][items[1]] = {"tag": items[2], "amount": items[3]}

    return record


if __name__ == "__main__":
    destination_users_db_cvs = parse_record_file(file_name="rc_20161212.csv")
    error_log_path = os.path.join(top, "bin", "compare_error.txt")

    if os.path.exists(error_log_path):
        os.remove(error_log_path)

    start_time = time.strftime('%Y.%m.%d_%H.%M.%S')

    print len(destination_users_db_cvs)
    print "================================\n\n"
    # print_dict(destination_users_db_cvs, 1)

    origin_users = get_all_data_from_source()
    print len(origin_users)
    print "================================\n\n"

    # 以下注释代码段主要用于检查数据源和目标数据的user_id交集以及差集
    #
    # 并集  s.intersection(t)  s & t
    # 合集  s.union(t)  s | t
    # 差集  s.difference(t)  s - t
    #
    common_set = set(origin_users.keys()) & set(destination_users_db_cvs.keys())
    print len(common_set)

    print len(set(origin_users.keys()) - set(destination_users_db_cvs.keys()))

    origin = dict(origin_users)
    destination = dict(destination_users_db_cvs)

    print origin.keys()[:5]
    print destination.keys()[:5]
    print

    for _key in origin:
        if _key in destination:
            destination.pop(_key)
        else:
            print _key

    print len(destination)

    print "#################################"

    print set(destination_users_db_cvs.keys()).issubset(set(origin_users.keys()))
    print

    print origin_users.keys()[1]
    print origin_users[origin_users.keys()[1]]
    print
    print destination_users_db_cvs.keys()[1]
    print destination_users_db_cvs[destination_users_db_cvs.keys()[1]]
    print

    error_list = list()
    index = 0
    for user_id, ledgers in origin_users.items():
        try:
            user_id = str(user_id)
            ledgers = dict(ledgers)
            index += 1
            print index, user_id
            assert user_id in destination_users_db_cvs, "%s not found from db recode" % user_id
            db_record = destination_users_db_cvs[user_id]
            for ledger_code, origin_value in ledgers.items():
                assert ledger_code in db_record, \
                    "user_id: %s, ledger_code: %s not found from destination" % (user_id, ledger_code)
                db_value = db_record[ledger_code]['amount']
                origin_value = decimal.Decimal(origin_value)
                db_value = decimal.Decimal(db_value)
                msg = "%s_%s : exp:act(%s/%s)" % (user_id, ledger_code, str(origin_value), str(db_value))
                # print msg
                assert not origin_value.compare(db_value), msg

            # 验证除了导入数据之外的另外的ledger_code对应的amount为零
            other_ledgers = list(set(db_record.keys()) - set(ledgers.keys()))
            for _other_legder in other_ledgers:
                assert len(other_ledgers) == 5, "%s %s %s which should be 5" % \
                                                (user_id, _other_legder, str(len(other_ledgers)))
                other_ledger_amount = decimal.Decimal(db_record[_other_legder]["amount"])
                assert not other_ledger_amount, "%s %s %s which should be 0" % \
                                                (user_id, _other_legder, str(other_ledger_amount))

        except BaseException, e:
            error_list.append(str(e))
        finally:
            pass

    print
    print "START : " + start_time
    print "ENDED : " + time.strftime('%Y.%m.%d_%H.%M.%S')
    print

    error_msg = "\n".join([str(a) for a in error_list])
    print error_msg
    print len(error_list)
    with open(error_log_path, "w+") as f:
        f.writelines(error_msg)



