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
# #formatter = logging.Formatter(logging_format)
handler = logging.FileHandler('console.log')
handler.setFormatter(logging.Formatter(logging_format))
logger = logging.getLogger()
logger.addHandler(handler)

ITERATIONS = config.iterations
THREADS = config.threads


# ITERATIONS = args.iterations
# THREADS = args.threads


def setup():
    # logger.debug('MEMORY USING: %s'% (psutil.Process(os.getpid()).memory_info().rss))
    # logger.debug('VIRTUAL: %s' % (psutil.Process(os.getpid()).memory_info().vms))
    # logger.debug(psutil.virtual_memory().available)
    # logger.debug((psutil.virtual_memory().total))
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


def print_console():
    msg = "Database Benchmark Driver Application v1 \n" \
          "----------------------------------------------------------------\n" \
          "MongoDB | 1: Insert   2: Find    3: Scan   4: Run*   5: Run*Live \n" \
          "MySQL   | 6: Insert   7: Select  8: Scan   9: Run*  10: Run*Live"
    print(u"\u2500" * 64, msg, u"\u2500" * 64, sep='\n')


def graph_results(points):
    print("nO!")


#######################
#   TEST SUITES
#######################

def bulk_insert_test_suite():
    # accumulate timing
    t1, t2, t3 = [], [], []

    # count inserted documents
    d1, d2, d3 = [], [], []

    # test names
    tests = ['mongo_bulk_insert', 'mysql_bulk_insert_u', 'mysql_bulk_insert_n']

    ###############################
    #  TEST 1: Mongo Bulk Insert  #
    ###############################
    for i in range(ITERATIONS):
        t, docs = mongo_db.bulk_insert()
        t1.append(t), d1.append(docs)

    ################################
    #  TEST 2: MySQL Bulk Insert U #
    ################################
    for i in range(ITERATIONS):
        t, docs = mysql_db.bulk_insert_universal_2()
        t2.append(t), d2.append(docs)

    ################################
    #  TEST 3: MySQL Bulk Insert N #
    ################################
    for i in range(ITERATIONS):
        t, docs = mysql_db.bulk_insert_normalized_2()
        t3.append(t), d3.append(docs)

    # result printout

    print("{} {}    mean({})={}".format(tests[0], d1.pop(), len(t1), statistics.mean(t1)))
    print("{} {}    mean({})={}".format(tests[1], d2.pop(), len(t2), statistics.mean(t2)))
    print("{} {}    mean({})={}".format(tests[2], d3.pop(), len(t3), statistics.mean(t3)))

    # log test times

    log_results("{}_{}".format(tests[0], d1.pop()), t1)
    log_results("{}_{}".format(tests[1], d2.pop()), t2)
    log_results("{}_{}".format(tests[2], d3.pop()), t3)


def insert_index_test_suite():
    # accumulate timing
    t1, t2, t3, t4 = [], [], [], []

    # test names
    tests = ['mongo_insert_indexed', 'mongo_insert_non_indexed', 'mysql_insert_indexed', 'mysql_insert_non_indexed']

    ########################################
    #  TEST 1: MonogDB Insert One Indexed  #
    ########################################
    for i in range(ITERATIONS): t1.append(mongo_db.insert_one_indexed(drop=True))

    ############################################
    #  TEST 2: MongoDB Insert One Non-Indexed  #
    ############################################
    for i in range(ITERATIONS): t2.append(mongo_db.insert_one_non_indexed(drop=True))

    #######################################
    #  TEST 3: MySQL Insert One Indexed   #
    #######################################
    for i in range(ITERATIONS): t3.append(mysql_db.universal_insert_one_with_indexing_2())

    ###########################################
    #  TEST 4: MySQL Insert One Non-Indexed   #
    ###########################################
    for i in range(ITERATIONS): t4.append(mysql_db.universal_insert_one_without_indexing_2())

    # result printout

    print("{}          mean={}".format(tests[0], statistics.mean(t1)))
    print("{}          mean={}".format(tests[1], statistics.mean(t2)))
    print("{}          mean={}".format(tests[2], statistics.mean(t3)))
    print("{}          mean={}".format(tests[3], statistics.mean(t4)))

    # log test times

    log_results(tests[0], t1)
    log_results(tests[1], t2)
    log_results(tests[2], t3)
    log_results(tests[3], t4)


def find_index_test_suite():
    # accumulate timing
    t1, t2, t3, t4 = [], [], [], []

    # test names
    tests = ['mongo_find_indexed', 'mongo_find_non_indexed', 'mysql_find_indexed', 'mysql_find_non_indexed']

    ########################################
    #  TEST 1: MonogDB Find Indexed  #
    ########################################
    for i in range(ITERATIONS): t1.append(mongo_db.find(True))

    ############################################
    #  TEST 2: MongoDB Find Non-Indexed  #
    ############################################
    #for i in range(ITERATIONS): t2.append(mongo_db.find(False))

    #######################################
    #  TEST 3: MySQL Find Indexed   #
    #######################################
    for i in range(ITERATIONS): t3.append(mysql_db.universal_select_with_indexing())

    ###########################################
    #  TEST 4: MySQL Find Non-Indexed   #
    ###########################################
    #for i in range(ITERATIONS): t4.append(mysql_db.universal_select_without_indexing())

    # result printout

    print("{}          mean={}".format(tests[0], statistics.mean(t1)))
    #print("{}          mean={}".format(tests[1], statistics.mean(t2)))
    print("{}          mean={}".format(tests[2], statistics.mean(t3)))
   # print("{}          mean={}".format(tests[3], statistics.mean(t4)))

    # log test times

    log_results(tests[0], t1)
    #log_results(tests[1], t2)
    log_results(tests[2], t3)
    #log_results(tests[3], t4)


def scan_test_suite():
    # accumulate timing
    t1, t2 = [], []

    # test names
    tests = ['mongo_scan', 'mysql_scan']

    ####################################
    #  TEST 1: MongoDB Scan  #
    ####################################

    for runs in range(ITERATIONS): t1.append(mongo_db.scan_all())

    ####################################
    #  TEST 2: MySQL Scan  #
    ####################################

    for runs in range(ITERATIONS): t2.append(mysql_db.scan_all())
    # result printout

    print("{}          mean={}".format(tests[0], statistics.mean(t1)))
    print("{}          mean={}".format(tests[1], statistics.mean(t2)))

    # log test times

    log_results(tests[0], t1)
    log_results(tests[0], t2)


def bulk_insert_test_suite_2():
    #  test_1: Mongo Bulk Insert
    test_1()
    #  test_7: MySQL Bulk Insert Universal
    test_7()
    #  test_8: MySQL Bulk Insert Normalized
    test_8()


def insert_index_test_suite_2():
    #  test_2: MongoDB Insert One Indexed
    test_2()
    #  test_3 MongoDB Insert One Non-Indexed
    test_3()
    #  test_9: MySQL Insert One Indexed
    test_9()
    #  test_10 MySQL Insert One Non-Indexed
    test_10()


def find_index_test_suite_2():
    #  test_4: MonogDB Find Indexed  #
    test_4()
    #  test_11: MySQL Select Indexed
    test_11()


def scan_test_suite_2():
    #  test_6: MongoDB Scan
    test_6()
    #  test_13: MySQL Scan
    test_13()

#######################
#   TEST PROCEDURES
#######################


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

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size = mongo_db.insert_one_indexed(drop=True)
        t1.append(t), d.append(size)

    log = 'test_2: mongo_db.insert_one_indexed(drop=True), doc_size={}, time_mean={}'
    print(log.format(size, statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)


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
    print(log.format(size,statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)


# test_8: mysql_db.bulk_insert_normalized()
def test_8():
    # times for each insert
    t1 = []
    d=[]

    # perform multiple test iterations
    for runs in range(ITERATIONS):
        t, size = mysql_db.bulk_insert_normalized_2()
        t1.append(t), d.append(size)

    log = 'test_8: mysql_db.bulk_insert_normalized(), doc_size={}, time_mean={}'
    print(log.format(size,statistics.mean(t1)))
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


#TODO: finish proper argument
parser = argparse.ArgumentParser(description='DBD - Database Benchmark Driver')
parser.add_argument('-t', '--test', help='Test suite to perform.', required=False, choices=['1', '2', '3', '4'])
parser.add_argument('-m', '--manual', help='Manual test to perform.', required=False, choices=['1','2','3','4','5','6','7','8','9','10','11','12','13'])
parser.add_argument('-d', '--debug', help='Debugger verbosity.', required=False, default='INFO',
                    choices=['INFO', 'DEBUG'])

parser.add_argument('-s', '--size', help='doc size {1=5MB, 2=50MB, 3=100MB, 4=500MB, 5=1GB, 6=MaxGB }', required=False, choices=['1', '2', '3', '4', '5', '6'])
parser.add_argument('-i', '--iterations', help='Number of iterations.', required=False)

args = parser.parse_args()


if args.test == '1':
    logger.info("PERFORMING OP 1: Bulk Insert Test Suite")
    bulk_insert_test_suite_2()
if args.test == '2':
    logger.info("PERFORMING OP 2: Insert Index Test Suite")
    insert_index_test_suite_2()
if args.test == '3':
    logger.info("PERFORMING OP 3: Find Index Test Suite")
    find_index_test_suite_2()
if args.test == '4':
    logger.info("PERFORMING OP 4: Scan Test Suite")
    scan_test_suite_2()

if args.manual == '1': test_1()
if args.manual == '2': test_2()
if args.manual == '3': test_3()
if args.manual == '4': test_4()
if args.manual == '5': test_5()
if args.manual == '6': test_6()
if args.manual == '7': test_7()
if args.manual == '8': test_8()
if args.manual == '9': test_9()
if args.manual == '10': test_10()
if args.manual == '11': test_11()
if args.manual == '12': test_12()
if args.manual == '13': test_13()


if __name__ == "__main__":
    # call main setup
    setup()

    # test connection to databases
    mongo_db.connect()
    mysql_db.connect()

   # mongo_db.bulk_insert_collections()
    test_7()

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
