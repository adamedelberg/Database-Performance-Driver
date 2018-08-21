# -*- coding: utf-8 -*-
"""
MySQL live database simulator
author: Adam Edelberg
date: 13 Aug 2018
"""

import threading
import json

import time
import pymysql

USER = 'root'
PASS = 'passQ123'
HOST = 'localhost'
DATABASE = 'benchmark_db'
DATABASE_INDEXED = 'benchmark_db_indexed'


class MyThread(threading.Thread):
    def __init__(self, thread_id, name):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = name

    def run(self):
        simulate_mysql(self.name)


def simulate_mysql(thread):
    print()


def main(n):
    # Create new threads and start them
    for x in range(n):
        mythread = MyThread(x, "Thread-{}".format(x + 1))
        mythread.start()
        mythread.join()

