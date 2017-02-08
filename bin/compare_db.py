#!/usr/bin/env python
# encoding: utf-8

import os
import sys
import time
import decimal
from common import get_all_data_from_source, get_all_data_from_destination
reload(sys)

top = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(top)


if __name__ == "__main__":
    error_log_path = os.path.join(top, "bin", "compare_db_error.txt")
    if os.path.exists(error_log_path):
        os.remove(error_log_path)

    start_time = time.strftime('%Y.%m.%d_%H.%M.%S')
    print start_time
    source_db_data = get_all_data_from_source()
    print time.strftime('%Y.%m.%d_%H.%M.%S')
    destination_db_data = get_all_data_from_destination()
    print time.strftime('%Y.%m.%d_%H.%M.%S')

    index = 0
    error_list = list()
    for user_id, ledgers in source_db_data.items():
        try:
            index += 1
            print index, user_id
            assert user_id in destination_db_data, "%s not found from db recode" % user_id
            db_record = dict(destination_db_data[user_id])
            for ledger_code, origin_value in ledgers.items():
                assert ledger_code in db_record, \
                    "user_id: %s, ledger_code: %s not found from destination" % (user_id, ledger_code)
                db_value = db_record[ledger_code]
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
                other_ledger_amount = decimal.Decimal(db_record[_other_legder])
                assert not other_ledger_amount, "%s %s %s which should be 0" % \
                                                (user_id, _other_legder, str(other_ledger_amount))

        except BaseException, e:
            error_list.append((user_id, str(e)))
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



