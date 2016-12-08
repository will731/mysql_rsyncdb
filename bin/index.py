#!/usr/bin/env python# -*- coding: UTF-8 -*-"""@author:Eric.xin""""""执行"""import osimport sysreload(sys)sys.setdefaultencoding("utf-8")parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))sys.path.append(parentdir)from Conf import d_configfrom Conf import s_configimport loggingimport timeimport threadingimport MySQLdb as mydbfrom t_thread import Threadpoolimport jsonfrom bin.source_data import S_dbimport jsonLogger = logging.getLogger(__name__)BASE_DIR = os.path.dirname(__file__)os_path=os.path.join(BASE_DIR,'logs/')def get_dataall():    #实例化S_db类,通过get_data方法获取查询的数据值    s = S_db(s_config.ip, s_config.user, s_config.pwd, s_config.db)    return s.get_data()"""定义基础类,用与连接数据库"""class Dest_db(object):    def __init__(self, host, user, password, databases):        self.__Data = get_dataall()        try:            self.host = host            self.user = user            self.password = password            self.databases = databases        except Exception, e:            print e,            logging.basicConfig(level=logging.INFO, filename='dest_connet.log',                                format='%(asctime)s  %(message)s')            logging.info(u"未连接到更新数据库")    #获取用户id,user_id    def get_user_id(self):        user_id_list=[]        for i in self.__Data['user_id']:            user_id_list.append(i)        return user_id_list    #获取可用余额    available_amount    def get_available_amount(self):        available_amount_list=[]        for i in self.__Data['available_amount']:            available_amount_list.append(i)        return available_amount_list    #获取冻结金额     freeze_amount    def get_freeze_amount(self):        freeze_amount_list=[]        for i in self.__Data['freeze_amount']:            freeze_amount_list.append(i)        return freeze_amount_list    #获取待收本金     ds_principal    def get_ds_principal(self):        ds_principal_list=[]        for i in self.__Data['ds_principal']:            ds_principal_list.append(i)        return ds_principal_list    #获取待收收益     ds_interest    def get_ds_interest(self):        ds_interest_list=[]        for i in self.__Data['ds_interest']:            ds_interest_list.append(i)        return ds_interest_list    #获取奖励金额     reward    def get_reward(self):        reward_list=[]        for i in self.__Data['reward']:            reward_list.append(i)        return reward_list    #/*更新银谷在线用户的可用余额户*/    def up_WeiChuJieHu(self):        count=0        for k in xrange(len(self.__Data['available_amount'])):            count += 1            available_amount = self.__Data['available_amount'][k]            user_id = self.__Data['user_id'][k]            sql = "update ledgers_entity, customer_business_entity " \                  "set ledgers_entity.amount =%s  where ledgers_entity.ledger_code = 'WeiChuJieHu' AND " \                  "ledgers_entity.customer_business_id = customer_business_entity.id and " \                  "customer_business_entity.application_id = 'YGONLINE' AND " \                  "customer_business_entity.user_id ='%s'" % (float(available_amount), str(user_id))            logging.basicConfig(level=logging.INFO, filename='update_all.log', format='%(asctime)s  %(message)s')            #logging.info(u"可用余额 %s %d条数据 " % (available_amount,int(count)))            #logging.info(u"用户id %s %d条数据" % (user_id,int(count)))            logging.basicConfig(level=logging.INFO, filename='erro.log', format='%(asctime)s  %(message)s')            try:                db_conet = mydb.connect(self.host, self.user, self.password, self.databases)                cursor = db_conet.cursor()                try:                    print u'可用余额:%s user_id:%s sql:%s' % (                        available_amount,                        user_id,                        sql,                    )                    logging.info(u"可用余额:%s user_id:%s sql:%s %d数据 " % (available_amount,user_id,sql, int(count)))                    cursor.execute(sql)                except Warning as e:                    pass                except Exception as e:                    print e,                    logging.basicConfig(level=logging.INFO, filename='erro.log', format='%(asctime)s  %(message)s')                    logging.info('%s'%e)                    logging.error(u'可用余额:%s user_id:%s sql:%s' % (                        available_amount,                        user_id,                        sql,                    ))                db_conet.commit()            except Exception as e:                logging.basicConfig(level=logging.ERROR, filename='erro.log',format='%(asctime)s  %(message)s')                logging.error(u"可用余额户:%s"%e)                logging.error(u'可用余额:%s user_id:%s sql:%s' % (                    available_amount,                    user_id,                    sql,                ))            finally:                pass                db_conet.close()    #/*更新银谷在线用户的待付本金户*/    def up_DaiFuBenJinHu(self):        count = 0        for k in xrange(len(self.__Data['available_amount'])):            count += 1            available_amount = self.__Data['available_amount'][k]            # print "---------",available_amount,count            user_id = self.__Data['user_id'][k]            # print "=========",user_id            sql = "update ledgers_entity,customer_business_entity" \                  " set ledgers_entity.amount=%s where ledgers_entity.ledger_code='DaiFuBenJinHu' AND " \                  "ledgers_entity.customer_business_id=customer_business_entity.id  and " \                  "customer_business_entity.application_id='YGONLINE' AND " \                  "customer_business_entity.user_id='%s'" %(float(available_amount), str(user_id))            logging.basicConfig(level=logging.INFO, filename='update_all.log', format='%(asctime)s  %(message)s')            #logging.info(u"待付本金 %s %d条数据 " % (available_amount, int(count)))            #logging.info(u"用户id %s %d条数据" % (user_id, int(count)))            # print u"可用余额", available_amount, count            # print u"用户id", user_id, count            logging.basicConfig(level=logging.INFO, filename='erro.log', format='%(asctime)s  %(message)s')            try:                db_conet = mydb.connect(self.host, self.user, self.password, self.databases)                cursor = db_conet.cursor()                try:                    print u'待付本金:%s user_id:%s sql:%s' % (                        available_amount,                        user_id,                        sql                    )                    logging.info(u"待付本金:%s user_id:%s sql:%s %d数据 " % (available_amount, user_id, sql, int(count)))                    cursor.execute(sql)                except Warning as e:                    pass                except Exception as e:                    print e,                    logging.basicConfig(level=logging.INFO, filename='erro.log', format='%(asctime)s  %(message)s')                    logging.info('%s' % e)                    logging.error(u'待付本金:%s user_id:%s sql:%s' % (                        available_amount,                        user_id,                        sql                    ))                db_conet.commit()            except Exception as e:                logging.basicConfig(level=logging.ERROR, filename='erro.log', format='%(asctime)s  %(message)s')                logging.error(u"待付本金-%s" % e)                logging.error(u'待付本金:%s user_id:%s sql:%s' % (                    available_amount,                    user_id,                    sql                ))            finally:                pass                db_conet.close()     #/*更新银谷在线用户的待付利息户*/    #/*更新银谷在线用户的代付利息*/    def up_DaiFuLiXiHu(self):        count = 0        for k in xrange(len(self.__Data['available_amount'])):            count += 1            available_amount = self.__Data['available_amount'][k]            # print "---------",available_amount,count            user_id = self.__Data['user_id'][k]            # print "=========",user_id            sql = "update ledgers_entity,customer_business_entity" \                  " set ledgers_entity.amount=%s where ledgers_entity.ledger_code='DaiFuLiXiHu' AND " \                  "ledgers_entity.customer_business_id=customer_business_entity.id  and " \                  "customer_business_entity.application_id='YGONLINE' AND " \                  "customer_business_entity.user_id='%s'" % (float(available_amount), str(user_id))            logging.basicConfig(level=logging.INFO, filename='update_all.log', format='%(asctime)s  %(message)s')            #logging.info(u"待付利息 %s %d条数据 " % (available_amount, int(count)))            #logging.info(u"用户id %s %d条数据" % (user_id, int(count)))            # print u"可用余额", available_amount, count            # print u"用户id", user_id, count            logging.basicConfig(level=logging.INFO, filename='erro.log', format='%(asctime)s  %(message)s')            try:                db_conet = mydb.connect(self.host, self.user, self.password, self.databases)                cursor = db_conet.cursor()                try:                    print u'待付利息:%s user_id:%s sql:%s' % (                        available_amount,                        user_id,                        sql                    )                    logging.info(u"待付利息:%s user_id:%s sql:%s %d数据 " % (available_amount, user_id, sql, int(count)))                    cursor.execute(sql)                except Warning as e:                    pass                except Exception as e:                    print e,                    logging.basicConfig(level=logging.INFO, filename='erro.log', format='%(asctime)s  %(message)s')                    logging.info('%s' % e)                    logging.error(u'待付利息:%s user_id:%s sql:%s' % (                        available_amount,                        user_id,                        sql                    ))                db_conet.commit()            except Exception as e:                logging.basicConfig(level=logging.ERROR, filename='erro.log', format='%(asctime)s  %(message)s')                logging.error(u"待付利息-%s" % e)                logging.error(u'待付利息:%s user_id:%s sql:%s' % (                    available_amount,                    user_id,                    sql                ))            finally:                pass                db_conet.close()    #/*更新银谷在线用户的营销户,奖励*/    def up_YingXiaoHu(self):        count = 0        for k in xrange(len(self.__Data['available_amount'])):            count += 1            available_amount = self.__Data['available_amount'][k]            # print "---------",available_amount,count            user_id = self.__Data['user_id'][k]            # print "=========",user_id            sql = "update ledgers_entity,customer_business_entity" \                  " set ledgers_entity.amount=%s where ledgers_entity.ledger_code='YingXiaoHu' AND " \                  "ledgers_entity.customer_business_id=customer_business_entity.id  and " \                  "customer_business_entity.application_id='YGONLINE' AND " \                  "customer_business_entity.user_id='%s'" % (float(available_amount), str(user_id))            logging.basicConfig(level=logging.INFO, filename='update_all.log', format='%(asctime)s  %(message)s')            #logging.info(u"营销用户 %s %d条数据 " % (available_amount, int(count)))            #logging.info(u"用户id %s %d条数据" % (user_id, int(count)))            logging.basicConfig(level=logging.INFO, filename='erro.log', format='%(asctime)s  %(message)s')            try:                db_conet = mydb.connect(self.host, self.user, self.password, self.databases)                cursor = db_conet.cursor()                try:                    print u'营销用户:%s user_id:%s sql:%s' % (                        available_amount,                        user_id,                        sql                    )                    logging.info(u"营销用户:%s user_id:%s sql:%s %d数据 " % (available_amount, user_id, sql, int(count)))                    cursor.execute(sql)                except Warning as e:                    pass                except Exception as e:                    print e,                    logging.basicConfig(level=logging.INFO, filename='erro.log', format='%(asctime)s  %(message)s')                    logging.info('%s' % e)                    logging.error(u'营销用户:%s user_id:%s sql:%s' % (                        available_amount,                        user_id,                        sql                    ))                db_conet.commit()            except Exception as e:                logging.basicConfig(level=logging.ERROR, filename='erro.log', format='%(asctime)s  %(message)s')                logging.error(u"营销用户:%s" % e)                logging.error(u'营销用户:%s user_id:%s sql:%s' % (                    available_amount,                    user_id,                    sql                ))            finally:                pass                db_conet.close()    #/ *更新银谷在线用户的提现冻结户 * /    def up_TiXianHu(self):        count = 0        for k in xrange(len(self.__Data['available_amount'])):            count += 1            available_amount = self.__Data['available_amount'][k]            # print "---------",available_amount,count            user_id = self.__Data['user_id'][k]            # print "=========",user_id            sql = "update ledgers_entity,customer_business_entity" \                  " set ledgers_entity.amount=%s where ledgers_entity.ledger_code='TiXianHu' AND " \                  "ledgers_entity.customer_business_id=customer_business_entity.id  and " \                  "customer_business_entity.application_id='YGONLINE' AND " \                  "customer_business_entity.user_id='%s'" % (float(available_amount), str(user_id))            logging.basicConfig(level=logging.INFO, filename='update_all.log', format='%(asctime)s  %(message)s')            #logging.info(u"冻结提现用户 %s %d条数据 " % (available_amount, int(count)))            #logging.info(u"用户id %s %d条数据" % (user_id, int(count)))            # print u"可用余额", available_amount, count            # print u"用户id", user_id, count            logging.basicConfig(level=logging.INFO, filename='erro.log', format='%(asctime)s  %(message)s')            try:                db_conet = mydb.connect(self.host, self.user, self.password, self.databases)                cursor = db_conet.cursor()                try:                    print u'冻结提现用户:%s user_id:%s sql:%s' % (                        available_amount,                        user_id,                        sql                    )                    logging.info(u"冻结提现用户:%s user_id:%s sql:%s %d数据 " % (available_amount, user_id, sql, int(count)))                    cursor.execute(sql)                except Warning as e:                    pass                except Exception as e:                    print e,                    logging.basicConfig(level=logging.INFO, filename='erro.log', format='%(asctime)s  %(message)s')                    logging.info('%s' % e)                    logging.error(u'冻结体现用户:%s user_id:%s sql:%s' % (                        available_amount,                        user_id,                        sql                    ))                db_conet.commit()            except Exception as e:                logging.basicConfig(level=logging.ERROR, filename='erro.log', format='%(asctime)s  %(message)s')                logging.error(u"冻结体现用户:%s" % e)                logging.error(u'冻结体现:%s user_id:%s sql:%s' % (                    available_amount,                    user_id,                    sql                ))            finally:                pass                db_conet.close()pool = Threadpool()def run(task,pool):    t = pool.get_thread()    obj = t(target=task, args=())    obj.start()if __name__=="__main__":    d = Dest_db(d_config.ip, d_config.user, d_config.pwd, d_config.db)    #print d.up_WeiChuJieHu()    #print d.up_DaiFuBenJinHu()    #print d.up_DaiFuLiXiHu()    #print d.    up_DaiFuLiXiHu()    #print d.up_YingXiaoHu()    start_time=time.time()    run(d.up_WeiChuJieHu, pool)    run(d.up_DaiFuBenJinHu, pool)    run(d.up_DaiFuLiXiHu, pool)    run(d.up_DaiFuLiXiHu, pool)    run(d.up_YingXiaoHu, pool)    end_time=time.time()    print end_time - start_time    """    #print d.up_available_amount()    #print d.get_user_id()    #测试获取的数据来源    print "d.get_user_id()",d.get_user_id()    print "d.get_available_amount()",d.get_available_amount()    print "d.get_ds_interest()",d.get_ds_interest()    print "d.get_ds_principal()",d.get_ds_principal()    print "d.get_freeze_amount()",d.get_freeze_amount()    print "d.get_reward()",d.get_reward()    """