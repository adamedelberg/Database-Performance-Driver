# -*- coding: utf-8 -*-
"""
Main driver
author: Adam Edelberg
date: 02 Aug 2018
"""

import csv
import logging
import threading
import argparse
import mongo_db
import mysql_db
import mongo_db_live
import mysql_db_live
import statistics
import config
# import matplotlib.pyplot as plt
import psutil
import time
import datetime
import os
import timeit

# console logging
logging_format = '%(levelname)s: %(asctime)s: %(name)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=logging_format)
handler = logging.FileHandler('console.log')
handler.setFormatter(logging.Formatter(logging_format))
logger = logging.getLogger()
logger.addHandler(handler)

ITERATIONS = config.iterations
THREADS = config.threads


def setup():
    # logger.debug('MEMORY USING: %s'% (psutil.Process(os.getpid()).memory_info().rss))
    # logger.debug('VIRTUAL: %s' % (psutil.Process(os.getpid()).memory_info().vms))
    # logger.debug(psutil.virtual_memory().available)
    # logger.debug((psutil.virtual_memory().total))
    # test connection to databases
    mongo_db.connect()
    mysql_db.connect()
    print()


def log_results(test_name, data):
    try:
        with open(r'logs.csv', 'a') as report:
            writer = csv.writer(report, dialect='excel')
            writer.writerow([test_name] + data)
            report.close()
        # logger.info('')
    except Exception as e:
        # logger.warning('' + e)
        print()


############################################################
#                   TEST SUITE PROCEDURES                  #
############################################################


def ts_bulk_insert():
    #  test_1: Mongo Bulk Insert
    test_1()
    #  test_14: Mongo Bulk Insert Collections
    test_14()
    #  test_7: MySQL Bulk Insert Universal
    test_7()
    #  test_8: MySQL Bulk Insert Normalized
    test_8()


def ts_insert_index():
    #  test_2: MongoDB Insert One Indexed
    test_2()
    #  test_3 MongoDB Insert One Non-Indexed
    test_3()
    #  test_9: MySQL Insert One Indexed
    test_9()
    #  test_10 MySQL Insert One Non-Indexed
    test_10()


def ts_find_index():
    #  test_4: MonogDB Find Indexed  #
    test_4()
    #  test_11: MySQL Select Indexed
    test_11()
    #  test_5: MonogDB Find Non Indexed  #
    test_5()
    #  test_12: MySQL Select Non Indexed
    test_12()


def ts_scan():
    #  test_6: MongoDB Scan
    test_6()
    #  test_13: MySQL Scan
    test_13()


############################################################
#                  MANUAL TEST PROCEDURES                  #
############################################################


# test_1: mongo_db.bulk_insert()
def test_1():
    # times for each insert
    t1 = []

    # size(s) of docs
    d = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size = mongo_db.bulk_insert()
        t1.append(t), d.append(size)

    log = 'test_1: mongo_db.bulk_insert(), doc_size={}, time_mean={}'
    print(log.format(size, statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)


# test_2: mongo_db.insert_one_indexed(drop=True)
def test_2():
    # times for each insert
    t1 = []

    # size(s) of docs
    d = []
    d2 = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size, size2 = mongo_db.insert_one_indexed(drop=True)
        t1.append(t)
        #d.append(size), d2.append(size2)

    log = 'test_2: mongo_db.insert_one_indexed(drop=True), db_size= {}, doc_size={}, time_mean={}'
    print(log.format(size2, size, statistics.mean(t1)))
    log_results(log[:-14].format(size2, size), t1)


# test_3: mongo_db.insert_one_non_indexed(drop=True)
def test_3():
    # times for each insert
    t1 = []

    # size(s) of docs
    d = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size = mongo_db.insert_one_non_indexed(drop=True)
        t1.append(t), d.append(size)

    log = 'test_3: mongo_db.insert_one_non_indexed(drop=True), doc_size={}, time_mean={}'
    print(log.format(size, statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)


# test_4: mongo_db.find(indexed=True)
def test_4():
    # times for each
    t1 = []

    # perform multiple test iterations
    for i in range(ITERATIONS): t1.append(mongo_db.find(indexed=True))

    log = 'test_4: mongo_db.find(indexed=True), time_mean={}'
    print(log.format(statistics.mean(t1)))
    log_results(log[:-14].format(), t1)


# test_5: mongo_db.find(indexed=False)
def test_5():
    # times for each
    t1 = []

    # perform multiple test iterations
    for i in range(ITERATIONS): t1.append(mongo_db.find(indexed=False))

    log = 'test_5: mongo_db.find(indexed=False), time_mean={}'
    print(log.format(statistics.mean(t1)))
    log_results(log[:-14].format(), t1)


# test_6: mongo_db.scan_all()
def test_6():
    # times for each
    t1 = []

    # perform multiple test iterations
    for i in range(ITERATIONS): t1.append(mongo_db.scan_all())

    log = 'test_6: mongo_db.scan_all(), time_mean={}'
    print(log.format(statistics.mean(t1)))
    log_results(log[:-14].format(), t1)


# test_7: mysql_db.bulk_insert_universal()
def test_7():
    # times for each insert
    t1 = []
    d = []
    # perform multiple test iterations
    for runs in range(ITERATIONS):
        t, size = mysql_db.bulk_insert_universal_2()
        t1.append(t), d.append(size)
    log = 'test_7: mysql_db.bulk_insert_universal(), doc_size={}, time_mean={}'
    print(log.format(size, statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)


# test_8: mysql_db.bulk_insert_normalized()
def test_8():
    # times for each insert
    t1 = []
    d = []

    # perform multiple test iterations
    for runs in range(ITERATIONS):
        t, size = mysql_db.bulk_insert_normalized_2()
        t1.append(t), d.append(size)

    log = 'test_8: mysql_db.bulk_insert_normalized(), doc_size={}, time_mean={}'
    print(log.format(size, statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)


# test_9: mysql_db.universal_insert_one_with_indexing()
def test_9():
    # times for each insert
    t1 = []

    # perform multiple test iterations
    for runs in range(ITERATIONS): t1.append(mysql_db.universal_insert_one_with_indexing_2())

    log = 'test_9: mysql_db.universal_insert_one_with_indexing(), time_mean={}'
    print(log.format(statistics.mean(t1)))
    log_results(log[:-14].format(), t1)


# test_10: mysql_db.universal_insert_one_without_indexing()
def test_10():
    # times for each insert
    t1 = []

    # perform multiple test iterations
    for runs in range(ITERATIONS): t1.append(mysql_db.universal_insert_one_without_indexing_2())

    log = 'test_10: mysql_db.universal_insert_one_without_indexing(), time_mean={}'
    print(log.format(statistics.mean(t1)))
    log_results(log[:-14].format(), t1)


# test_11: mysql_db.universal_select_with_indexing()
def test_11():
    # times for each insert
    t1 = []

    # perform multiple test iterations
    for i in range(ITERATIONS): t1.append(mysql_db.universal_select_with_indexing())

    log = 'test_11: mysql_db.universal_select_with_indexing(), time_mean={}'
    print(log.format(statistics.mean(t1)))
    log_results(log[:-14].format(), t1)


# test_12: mysql_db.universal_select_without_indexing()
def test_12():
    # times for each insert
    t1 = []

    # perform multiple test iterations
    for i in range(ITERATIONS): t1.append(mysql_db.universal_select_without_indexing())

    log = 'test_12: mysql_db.universal_select_without_indexing(), time_mean={}'
    print(log.format(statistics.mean(t1)))
    log_results(log[:-14].format(), t1)


# test_13: mysql_db.scan_all()
def test_13():
    # times for each insert
    t1 = []

    # perform multiple test iterations
    for i in range(ITERATIONS): t1.append(mysql_db.scan_all())

    log = 'test_13: mysql_db.scan_all(), time_mean={}'
    print(log.format(statistics.mean(t1)))
    log_results(log[:-14].format(), t1)


# test_14: mongo_db.bulk_insert_collections()
def test_14():
    # times for each insert
    t1 = []

    # size(s) of docs
    d = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size = mongo_db.bulk_insert_collections()
        t1.append(t), d.append(size)

    log = 'test_14: mongo_db.bulk_insert_collections(), doc_size={}, time_mean={}'
    print(log.format(size, statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)


# TODO: finish proper argument
parser = argparse.ArgumentParser(description='DBD - Database Benchmark Driver')

parser.add_argument('-ts', '--test_suite', help='Select a test suite to perform.', required=False, type=int,
                    choices=[1, 2, 3, 4])

parser.add_argument('-t', '--test', help='Select a manual test to perform.', required=False, type=int,
                    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14])

parser.add_argument('-s', '--size', help='doc size {1=5MB, 2=50MB, 3=100MB, 4=500MB, 5=1GB, 6=MaxGB }', default=1, type=int,
                    required=False, choices=[1, 2, 3, 4, 5, 6])

parser.add_argument('-i', '--iterations', help='Number of iterations.', type=int, required=False, default=1)


parser.add_argument('-d', '--debug', help='Debugger verbosity.', required=False, default='v',
                    choices=['v', 'vv', 'vvv'])


args = parser.parse_args()

#ITERATIONS=args.iterations

# if args.size ==1:
#     logger.info("size=5MB")
# if args.size ==2:
#     logger.info("size=50MB")
#     data = '../parsed_data/e3-500MB.json'
# if args.size ==3:
#     data = '../parsed_data/e3-500MB.json'
# if args.size ==4:
#     data = '../parsed_data/e3-500MB.json'
# if args.size ==5:
#     data = '../parsed_data/e3-1GB.json'


if args.test_suite == 1:
    logger.info("PERFORMING OP 1: Bulk Insert Test Suite")
    ts_bulk_insert()
if args.test_suite == 2:
    logger.info("PERFORMING OP 2: Insert Index Test Suite")
    ts_insert_index()
if args.test_suite == 3:
    logger.info("PERFORMING OP 3: Find Index Test Suite")
    ts_find_index()
if args.test_suite == 4:
    logger.info("PERFORMING OP 4: Scan Test Suite")
    ts_scan()

if args.test == 1: test_1()
if args.test == 2: test_2()
if args.test == 3: test_3()
if args.test == 4: test_4()
if args.test == 5: test_5()
if args.test == 6: test_6()
if args.test == 7: test_7()
if args.test == 8: test_8()
if args.test == 9: test_9()
if args.test == 10: test_10()
if args.test == 11: test_11()
if args.test == 12: test_12()
if args.test == 13: test_13()
if args.test == 14: test_14()

if __name__ == "__main__":
    # call main setup
    setup()
    #mongo_db.bulk_insert_collections()
#    test_14()


class DatabaseThreads(threading.Thread):
    def __init__(self, thread_id, database):
        """
        :type thread_id: int
        :type database: int
        """
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.database = database

    def run(self):
        if self.database == 1:
            mongo_db_live.main(THREADS)
        elif self.database == 2:
            mysql_db_live.main(THREADS)
            print('hi')


class App:
  __conf = {
    "username": "",
    "password": "",
    "MYSQL_PORT": 3306,

  }
  __setters = ["username", "password"]

  @staticmethod
  def config(name):
    return App.__conf[name]

  @staticmethod
  def set(name, value):
    if name in App.__setters:
      App.__conf[name] = value
    else:
      raise NameError("Name not accepted in set() method")