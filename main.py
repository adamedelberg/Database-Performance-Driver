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
logging.basicConfig(level=logging.WARNING, format=logging_format)
handler = logging.FileHandler('console.log')
handler.setFormatter(logging.Formatter(logging_format))
logger = logging.getLogger()
logger.addHandler(handler)

ITERATIONS = config.iterations
THREADS = config.threads

DATABASE = config.database
DATABASE_INDEXED = config.indexed_database
COLLECTION = config.collection
DOCUMENT = config.document
DOCUMENT_DICT = config.document_dict
DOCUMENT_SINGLE = config.document_single
DATABASE_COLLECTION = config.collection_database


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
    t1_mongo_db_bulk_insert()
    #  test_14: Mongo Bulk Insert Collections
    t14_mongo_db_bulk_insert_collections()
    #  test_7: MySQL Bulk Insert Universal
    t7_mysql_db_bulk_insert_universal()
    #  test_8: MySQL Bulk Insert Normalized
    t8_mysql_db_bulk_insert_normalized()


def ts_insert_index():
    #  test_2: MongoDB Insert One Indexed
    t2_mongo_db_insert_one_indexed()
    #  test_3 MongoDB Insert One Non-Indexed
    t3_mongo_db_insert_one_non_indexed()
    #  test_9: MySQL Insert One Indexed
    t9_mysql_db_insert_one_indexed()
    #  test_10 MySQL Insert One Non-Indexed
    t10_mysql_db_insert_one_non_indexed()


def ts_find_index():
    mongo_db.bulk_insert(doc_path=DOCUMENT_DICT,indexed=True)
    mysql_db.bulk_insert_universal(doc_path=DOCUMENT, indexed=True)

    mongo_db.bulk_insert(doc_path=DOCUMENT_DICT, indexed=False)
    mysql_db.bulk_insert_universal(doc_path=DOCUMENT, indexed=False)

    #  test_4: MonogDB Find Indexed  #
    t4_mongo_db_find_indexed()
    #  test_5: MonogDB Find Non Indexed  #
    t5_mongo_db_find_non_indexed()
    #  test_11: MySQL Select Indexed
    #t11_mysql_db_universal_select_indexed()
    #  test_12: MySQL Select Non Indexed
    #t12_mysql_db_universal_select_non_indexed()
    t11_mysql_db_universal_select_indexed()
    t12_mysql_db_universal_select_non_indexed()


def ts_scan():
    mongo_db.bulk_insert(doc_path=DOCUMENT_DICT,indexed=False)

    mysql_db.bulk_insert_universal(doc_path=DOCUMENT, indexed=False)

    #  test_6: MongoDB Scan
    t6_mongo_db_scan()
    #  test_13: MySQL Scan
    t13_mysql_db_scan()


############################################################
#                  MANUAL TEST PROCEDURES                  #
############################################################


# test_1: mongo_db.bulk_insert()
def t1_mongo_db_bulk_insert():
    # times for each insert
    t1 = []

    # size(s) of docs
    d = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size = mongo_db.bulk_insert(doc_path=DOCUMENT_DICT, indexed=False)
        t1.append(t), d.append(size)

    log = 'test_1: mongo_db.bulk_insert(), doc_size={}, time_mean={}'
    print(log.format(size, statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)


# test_2: mongo_db.insert_one_indexed(drop=True)
def t2_mongo_db_insert_one_indexed():
    # times for each insert
    t1 = []

    # size(s) of docs
    d = []
    d2 = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size, size2 = mongo_db.insert_one_indexed(drop=True, doc_path=DOCUMENT_DICT)
        t1.append(t)
        #d.append(size), d2.append(size2)

    log = 'test_2: mongo_db.insert_one_indexed(drop=True), db_size= {}, doc_size={}, time_mean={}'
    print(log.format(size2, size, statistics.mean(t1)))
    log_results(log[:-14].format(size2, size), t1)


# test_3: mongo_db.insert_one_non_indexed(drop=True)
def t3_mongo_db_insert_one_non_indexed():
    # times for each insert
    t1 = []

    # size(s) of docs
    d = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size, size2 = mongo_db.insert_one_non_indexed(drop=True, doc_path=DOCUMENT_DICT)
        t1.append(t)#, d.append(size)

    log = 'test_3: mongo_db.insert_one_non_indexed(drop=True), db_size= {}, doc_size={}, time_mean={}'
    print(log.format(size2, size, statistics.mean(t1)))
    log_results(log[:-14].format(size2, size), t1)

############################
# OP 3: Find Indexed Tests
###########################

# test_4: mongo_db.find(indexed=True)
def t4_mongo_db_find_indexed():
    # times for each
    t1 = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size = mongo_db.find(doc_path=DOCUMENT_DICT, indexed=True)
        t1.append(t)

    #log = 'test_4: mongo_db.find(indexed=True), time_mean={}'

    log = 'test_4: mongo_db.find(indexed=True), db_size= {}, time_mean={}'
    print(log.format(size, statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)

    #print(log.format(statistics.mean(t1)))
    #log_results(log[:-14].format(), t1)


# test_5: mongo_db.find(indexed=False)
def t5_mongo_db_find_non_indexed():
    # times for each
    t1 = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size = mongo_db.find(doc_path=DOCUMENT_DICT, indexed=False)
        t1.append(t)
    log = 'test_5: mongo_db.find(indexed=False), time_mean={}'
    log = 'test_5: mongo_db.find(indexed=False), db_size= {}, time_mean={}'

    print(log.format(size, statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)

####################
# OP 4: Scan Tests
####################

# test_6: mongo_db.scan_all()
def t6_mongo_db_scan():
    # times for each
    t1 = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, scanned = mongo_db.scan_all(doc_path=DOCUMENT_DICT)
        t1.append(t)

    log = 'test_6: mongo_db.scan_all(), time_mean={}, db_size={}, scanned={}'
    db_size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))
    print(log.format(statistics.mean(t1),db_size,scanned))
    log_results(log.format(statistics.mean(t1), db_size, scanned), t1)


# test_13: mysql_db.scan_all()
def t13_mysql_db_scan():
    # times for each insert
    t1 = []

    for i in range(ITERATIONS):
        t, scanned = mysql_db.scan_all()
        t1.append(t)

    log = 'test_13: mysql_db.scan_all(), time_mean={}, db_size={}, scanned={}'
    db_size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))

    print(log.format(statistics.mean(t1),db_size, scanned))
    log_results(log.format(statistics.mean(t1), db_size, scanned), t1)






# test_7: mysql_db.bulk_insert_universal()
def t7_mysql_db_bulk_insert_universal():
    # times for each insert
    t1 = []
    d = []
    # perform multiple test iterations
    for runs in range(ITERATIONS):
        t, size = mysql_db.bulk_insert_universal(doc_path=DOCUMENT)
        t1.append(t), d.append(size)
    log = 'test_7: mysql_db.bulk_insert_universal(), doc_size={}, time_mean={}'
    print(log.format(size, statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)


# test_8: mysql_db.bulk_insert_normalized()
def t8_mysql_db_bulk_insert_normalized():
    # times for each insert
    t1 = []
    d = []

    # perform multiple test iterations
    for runs in range(ITERATIONS):
        t, size = mysql_db.bulk_insert_normalized()
        t1.append(t), d.append(size)

    log = 'test_8: mysql_db.bulk_insert_normalized(), doc_size={}, time_mean={}'
    print(log.format(size, statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)


# test_9: mysql_db.universal_insert_one_with_indexing()
def t9_mysql_db_insert_one_indexed():
    # times for each insert
    t1 = []

    # perform multiple test iterations
    for runs in range(ITERATIONS):
        t, size, size2 = mysql_db.universal_insert_one_with_indexing_2()
        t1.append(t)
        #t1.append(mysql_db.universal_insert_one_with_indexing_2())

    log = 'test_9: mysql_db.universal_insert_one_with_indexing(), db_size= {}, doc_size={}, time_mean={}'
    print(log.format(size2, size, statistics.mean(t1)))
    log_results(log[:-14].format(size2, size), t1)


# test_10: mysql_db.universal_insert_one_without_indexing()
def t10_mysql_db_insert_one_non_indexed():
    # times for each insert
    t1 = []

    # perform multiple test iterations
    for runs in range(ITERATIONS):
        t, size, size2 = mysql_db.universal_insert_one_without_indexing_2()
        t1.append(t)
    #t1.append(mysql_db.universal_insert_one_without_indexing_2())

    log = 'test_10: mysql_db.universal_insert_one_without_indexing(), db_size= {}, doc_size={}, time_mean={}'
    print(log.format(size2, size, statistics.mean(t1)))
    log_results(log[:-14].format(size2, size), t1)






# test_14: mongo_db.bulk_insert_collections()
def t14_mongo_db_bulk_insert_collections():
    # times for each insert
    t1 = []

    # size(s) of docs
    d = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size = mongo_db.bulk_insert_collections(doc_path=DOCUMENT_DICT)
        t1.append(t), d.append(size)

    log = 'test_14: mongo_db.bulk_insert_collections(), doc_size={}, time_mean={}'
    print(log.format(size, statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)


def t11_mysql_db_universal_select_indexed():
    # times for each insert
    t1 = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size= mysql_db.universal_select(indexed=True, doc_path=DOCUMENT)
        t1.append(t)

    log = 'test_15: mysql_db.universal_select(indexed=True), db_size= {}, time_mean={}'

    print(log.format(size, statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)


def t12_mysql_db_universal_select_non_indexed():
    # times for each insert
    t1 = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size= mysql_db.universal_select(indexed=False, doc_path=DOCUMENT)
        t1.append(t)

    log = 'test_16: mysql_db.universal_select(indexed=False), db_size= {}, time_mean={}'

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

if args.test == 1: t1_mongo_db_bulk_insert()
if args.test == 2: t2_mongo_db_insert_one_indexed()
if args.test == 3: t3_mongo_db_insert_one_non_indexed()
if args.test == 4: t4_mongo_db_find_indexed()
if args.test == 5: t5_mongo_db_find_non_indexed()
if args.test == 6: t6_mongo_db_scan()
if args.test == 7: t7_mysql_db_bulk_insert_universal()
if args.test == 8: t8_mysql_db_bulk_insert_normalized()
if args.test == 9: t9_mysql_db_insert_one_indexed()
if args.test == 10: t10_mysql_db_insert_one_non_indexed()
if args.test == 11: t11_mysql_db_universal_select_indexed()
if args.test == 12: t12_mysql_db_universal_select_non_indexed()
if args.test == 13: t13_mysql_db_scan()
if args.test == 14: t14_mongo_db_bulk_insert_collections()

if __name__ == "__main__":
    # call main setup
    setup()
    ts_bulk_insert()
    ts_insert_index()
    ts_find_index()
    ts_scan()

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