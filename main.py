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

DATABASE = config.database
DATABASE_INDEXED = config.indexed_database
COLLECTION = config.collection
DOCUMENT = config.document
DOCUMENT_DICT = config.document_dict
DOCUMENT_SINGLE = config.document_single
DATABASE_COLLECTION = config.collection_database


def log_results(test_name, data):
    try:
        with open(r'logs.csv', 'a') as report:
            writer = csv.writer(report, dialect='excel')
            writer.writerow([test_name] + data)
            report.close()
    except csv.Error as code:
        logger.info("Logger: {}".format(code))


############################################################
#                   TEST SUITE PROCEDURES                  #
############################################################


def ts_bulk_insert():
    test_mongo_db_bulk_insert()
    test_mongo_db_bulk_insert_collections()

    test_mysql_db_bulk_insert_universal()
    test_mysql_db_bulk_insert_normalized()


def ts_insert_index():
    test_mongo_db_insert_one(indexed=True)
    test_mongo_db_insert_one(indexed=False)

    test_mysql_db_insert_one(indexed=True)
    test_mysql_db_insert_one(indexed=False)


def ts_find_index():
    mongo_db.bulk_insert(doc_path=DOCUMENT_DICT,indexed=True, drop_on_start=True, drop_on_exit=False)
    mongo_db.bulk_insert(doc_path=DOCUMENT_DICT, indexed=False, drop_on_start=True, drop_on_exit=False)

    mysql_db.bulk_insert_universal(doc_path=DOCUMENT, indexed=True)
    mysql_db.bulk_insert_universal(doc_path=DOCUMENT, indexed=False)

    test_mongo_db_find(indexed=True)
    test_mongo_db_find(indexed=False)

    test_mysql_db_select(indexed=True)
    test_mysql_db_select(indexed=False)


def ts_scan():
    mongo_db.bulk_insert(doc_path=DOCUMENT_DICT,indexed=False, drop_on_start=True, drop_on_exit=False)
    mysql_db.bulk_insert_universal(doc_path=DOCUMENT, indexed=False)

    test_mongo_db_scan()
    test_mysql_db_scan()


############################################################
#                  MANUAL TEST PROCEDURES                  #
############################################################

###########################
# OP 1: Bulk Insert Tests
###########################

def test_mongo_db_bulk_insert():
    # times for each insert
    t1 = []

    # size(s) of docs
    d = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size = mongo_db.bulk_insert(doc_path=DOCUMENT_DICT, indexed=False, drop_on_start=True)
        t1.append(t), d.append(size)

    log = 'test_1: mongo_db.bulk_insert(), doc_size={}, time_mean={}'
    print(log.format(size, statistics.mean(t1)))
    log_results(log[:-14].format(size), t1)


def test_mongo_db_bulk_insert_collections():
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


def test_mysql_db_bulk_insert_universal():
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


def test_mysql_db_bulk_insert_normalized():
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


##############################
# OP 2: Indexed Insert Tests
##############################

def test_mongo_db_insert_one(indexed):
    # times for each insert
    t1 = []

    # size(s) of docs
    d = []
    r = []
    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size, size2, run = mongo_db.insert_one(indexed=indexed, drop_on_start=True, doc_path=DOCUMENT_DICT)
        t1.append(t), r.append(run)
    log = 'test_: mongo_db.insert_one, indexed={}, db_size= {}, doc_size={}, time_mean={}, insert_time={}'
    print(log.format(indexed, size2, size, statistics.mean(t1), statistics.mean(r)))
    log_results(log[:-14].format(indexed, size2, size, statistics.mean(r)), t1)


def test_mysql_db_insert_one(indexed):
    # times for each insert
    t1 = []
    r = []
    # perform multiple test iterations
    for runs in range(ITERATIONS):
        t, size, size2, run = mysql_db.insert_one(indexed)
        t1.append(t), r.append(run)
    log = 'test_9: mysql_db.universal_insert_one indexed={}, db_size= {}, doc_size={}, time_mean={}, insert_time={}'
    print(log.format(indexed,size2, size, statistics.mean(t1), statistics.mean(r)))
    log_results(log[:-14].format(indexed, size2, size, statistics.mean(r)), t1)


############################
# OP 3: Find Indexed Tests
############################

def test_mongo_db_find(indexed):
    # times for each
    t1 = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size = mongo_db.find(doc_path=DOCUMENT_DICT, indexed=indexed)
        t1.append(t)

    log = 'test_4: mongo_db.find, indexed={}, db_size= {}, time_mean={}'
    print(log.format(indexed, size, statistics.mean(t1)))
    log_results(log[:-14].format(indexed, size), t1)


def test_mysql_db_select(indexed):
    # times for each insert
    t1 = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, size= mysql_db.select(indexed=indexed, doc_path=DOCUMENT)
        t1.append(t)

    log = 'test_11: mysql_db.select, indexed={}, db_size= {}, time_mean={}'

    print(log.format(indexed, size, statistics.mean(t1)))
    log_results(log[:-14].format(indexed, size), t1)


####################
# OP 4: Scan Tests
####################

def test_mongo_db_scan():
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


def test_mysql_db_scan():
    # times for each insert
    t1 = []

    for i in range(ITERATIONS):
        t, scanned = mysql_db.scan_all()
        t1.append(t)

    log = 'test_13: mysql_db.scan_all(), time_mean={}, db_size={}, scanned={}'
    db_size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))

    print(log.format(statistics.mean(t1),db_size, scanned))
    log_results(log.format(statistics.mean(t1), db_size, scanned), t1)





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

if args.test == 1: test_mongo_db_bulk_insert()
if args.test == 2: test_mongo_db_insert_one(indexed=True)
if args.test == 3: test_mongo_db_insert_one(indexed=False)
if args.test == 4: test_mongo_db_find(indexed=True)
if args.test == 5: test_mongo_db_find(indexed=False)
if args.test == 6: test_mongo_db_scan()
if args.test == 7: test_mysql_db_bulk_insert_universal()
if args.test == 8: test_mysql_db_bulk_insert_normalized()
if args.test == 9: test_mysql_db_insert_one(indexed=True)
if args.test == 10: test_mysql_db_insert_one(indexed=False)
if args.test == 11: test_mysql_db_select(indexed=True)
if args.test == 12: test_mysql_db_select(indexed=False)
if args.test == 13: test_mysql_db_scan()
if args.test == 14: test_mongo_db_bulk_insert_collections()

if __name__ == "__main__":
    # call connect to check access to databases
    mongo_db.connect(host=config.host, port=config.port)
    mysql_db.connect(host=config.mysql_host, port=config.mysql_port, user=config.username, password=config.password,
                     database=config.database)

    #ts_bulk_insert()
    #ts_insert_index()
    ts_find_index()
    #ts_scan()

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
