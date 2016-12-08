#!/usr/bin/env python# -*- coding: UTF-8 -*-"""@author:Eric.xin"""import osimport sysreload(sys)sys.setdefaultencoding("utf-8")parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))sys.path.append(parentdir)import queueimport threadingimport timeclass Threadpool:    """    自定义线程池    """    def __init__(self,maxsize=100):        """        初始化        :param maxsize: 队列存放数据的个数        """        self.maxsize = maxsize        # 创建一个队列        self._q = queue.Queue(maxsize)        # maxsize多大就创建一个线程对象        for i in range(maxsize):            self._q.put(threading.Thread)    def get_thread(self):        """        获取一个线程对象        等同于创建了一线线程对象t = threading.Thread        :return:        """        return self._q.get()    def add_thread(self):        """        添加一个线程对象到队列中        :return:        """        self._q.put(threading.Thread)