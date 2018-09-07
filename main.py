# -*- coding: utf-8 -*-
"""
Main driver
author: Adam Edelberg
date: 02 Aug 2018
"""

import os
import csv
import time
import config
import logging
import mongo_db
import mysql_db
import argparse
import threading
import statistics

ITERATIONS = config.default_iterations
THREADS = config.default_threads

DATABASE = config.database
DATABASE_INDEXED = config.indexed_database
DATABASE_COLLECTION = config.collection_database

COLLECTION = config.collection
PATH = config.document
SINGLE = config.single
DEBUG_LEVEL = config.DEBUG_LEVEL

# console logging
logging_format = '%(levelname)s: %(asctime)s: %(name)s: %(message)s'
logging.basicConfig(level=DEBUG_LEVEL, format=logging_format)
handler = logging.FileHandler('console.log')
handler.setFormatter(logging.Formatter(logging_format))
logger = logging.getLogger()
logger.addHandler(handler)


def log_res(tag, data):
    """

    :param tag:
    :param data:
    :return:
    """
    # print test data to console
    print(tag)
    timestr = time.strftime("%H:%M:%S", time.localtime())

    try:
        # logs get appended to the same file
        with open(r'logs.csv', 'a') as report:
            writer = csv.writer(report, dialect='excel')
            writer.writerow([timestr]+[tag] + data)
            report.close()
    except csv.Error as code:
        logger.info("Logger: {}".format(code))


############################################################
#                  MANUAL TEST PROCEDURES                  #
############################################################


def test_mongo_db_bulk_insert(indexed, iterations=ITERATIONS):
    # timing and metric variables
    times = []
    # perform multiple test iterations
    for i in range(iterations):
        t, size = mongo_db.bulk_insert(path=PATH,indexed=False, drop_on_start=True)
        times.append(t)

    # log results
    log = 'mongo_db.bulk_insert: doc_size={}, time_mean={}'
    log_res(log.format(size, statistics.mean(times)), times)
def test_mongo_db_bulk_insert_collections(indexed, iterations=ITERATIONS):
    times = []

    for i in range(iterations):
        t, doc_size = mongo_db.bulk_insert_collections(path=PATH, indexed=False, drop_on_start=True)
        times.append(t)

    log = 'mongo_db.bulk_insert_collections: doc_size={}, time_mean={}'.format(doc_size, statistics.mean(times))
    log_res(log, times)
def test_mysql_db_bulk_insert_universal(indexed, iterations=ITERATIONS):
    times = []

    for runs in range(iterations):
        t, size = mysql_db.bulk_insert_universal(path=PATH, indexed=False)
        times.append(t)

    log = 'mysql_db.bulk_insert_universal: doc_size={}, time_mean={}'
    log_res(log.format(size, statistics.mean(times)), times)
def test_mysql_db_bulk_insert_normalized(indexed, iterations=ITERATIONS):
    times = []

    for runs in range(iterations):
        t, doc_size = mysql_db.bulk_insert_normalized(path=PATH)
        times.append(t)

    log = 'mysql_db.bulk_insert_normalized: doc_size={}, time_mean={}'
    log_res(log.format(doc_size, statistics.mean(times)), times)


def test_mongo_db_bulk_insert_one(indexed, iterations=ITERATIONS):
    # timing and metric variables
    times = []

    # perform multiple test iterations
    for i in range(iterations):
        t, doc_size = mongo_db.bulk_insert_one(path=PATH, drop_on_start=True)
        times.append(t)

    # log results
    log = 'mongo_db.bulk_insert_one: doc_size={}, time_mean={}'
    log_res(log.format(doc_size, statistics.mean(times)), times)
def test_mongo_db_bulk_insert_one_collections(indexed, iterations=ITERATIONS):
    # timing and metric variables
    times = []

    # perform multiple test iterations
    for i in range(iterations):
        t, doc_size = mongo_db.bulk_insert_one_collections(path=PATH, drop_on_start=True)
        times.append(t)

    # log results
    log = 'mongo_db.bulk_insert_one_collections: doc_size={}, time_mean={}'
    log_res(log.format(doc_size, statistics.mean(times)), times)
def test_mysql_db_bulk_insert_one_universal(indexed, iterations=ITERATIONS):
    # timing and metric variables
    times = []

    # perform multiple test iterations
    for i in range(iterations):
        t, doc_size = mysql_db.bulk_insert_one_universal(path=PATH, indexed=False)
        times.append(t)

    # log results
    log = 'mysql_db.bulk_insert_one_universal: doc_size={}, time_mean={}'
    log_res(log.format(doc_size, statistics.mean(times)), times)
def test_mysql_db_bulk_insert_one_normalized(indexed, iterations=ITERATIONS):
    # timing and metric variables
    times = []

    # perform multiple test iterations
    for i in range(iterations):
        t, doc_size = mysql_db.bulk_insert_one_normalized(path=PATH, indexed=False)
        times.append(t)

    # log results
    log = 'mysql_db.bulk_insert_one_normalized: doc_size={}, time_mean={}'
    log_res(log.format(doc_size, statistics.mean(times)), times)


def test_mongo_db_insert_one(indexed, iterations):
    times, bulk = [], []

    for i in range(iterations):
        t, db_size, doc_size, bulk_insert_time = mongo_db.insert_one(indexed=indexed, drop_on_start=True,
                                                                     path=PATH)
        times.append(t), bulk.append(bulk_insert_time)

    log = 'mongo_db.insert_one: indexed={}, db_size= {}, doc_size={}, time_mean={}, insert_time={}'
    log_res(log.format(indexed, doc_size, db_size, statistics.mean(times), statistics.mean(bulk)), times)
def test_mongo_db_insert_one_collections(indexed, iterations ):
    times, bulk = [], []

    for i in range(iterations):
        t, db_size, doc_size, bulk_insert_time = mongo_db.insert_one_collections(indexed=indexed, drop_on_start=True, path=PATH)
        times.append(t), bulk.append(bulk_insert_time)

    log = 'mongo_db.insert_one_collections: indexed={}, db_size= {}, doc_size={}, time_mean={}, insert_time={}'
    log_res(log.format(indexed, doc_size, db_size, statistics.mean(times), statistics.mean(bulk)), times)
def test_mysql_db_insert_one_universal(indexed, iterations):
    times = []

    for runs in range(iterations):
        t, db_size, doc_size = mysql_db.insert_one_universal(path=PATH, indexed=indexed)
        times.append(t)

    log = 'mysql_db.universal_insert_one: indexed={}, db_size= {}, doc_size={}, time_mean={}'
    log_res(log.format(indexed, doc_size, db_size, statistics.mean(times)), times)

def test_mysql_db_insert_one_normalized(indexed, iterations):
    times = []

    for runs in range(iterations):
        t, db_size, doc_size = mysql_db.insert_one_normalized(path=PATH,indexed=indexed)
        times.append(t)

    log = 'mysql_db.universal_insert_one_normalized: indexed={}, db_size= {}, doc_size={}, time_mean={}'
    log_res(log.format(indexed, doc_size, db_size, statistics.mean(times)), times)


def test_mongo_db_find(indexed, iterations):
    mongo_db.bulk_insert(path=PATH, indexed=indexed, drop_on_start=True, drop_on_exit=False)

    count=0
    times = []

    for i in range(iterations):
        t, count = mongo_db.find(indexed=indexed)
        times.append(t)

    log = 'mongo_db.find: indexed={}, count= {}, time_mean={}'
    log_res(log.format(indexed, count, statistics.mean(times)), times)
def test_mongo_db_find_collections(indexed, iterations):
    mongo_db.bulk_insert_collections(path=PATH, indexed=indexed, drop_on_start=True, drop_on_exit=False)

    count=0
    times = []

    for i in range(iterations):
        t, count = mongo_db.find_collections(indexed=indexed)
        times.append(t)

    log = 'mongo_db.find_collections: indexed={}, count= {}, time_mean={}'
    log_res(log.format(indexed, count, statistics.mean(times)), times)


def test_mysql_db_select_universal(indexed, iterations):
    mysql_db.bulk_insert_universal(path=PATH, indexed=indexed)
    times = []

    for i in range(iterations):
        t, count = mysql_db.select_universal(indexed=indexed, path=PATH)
        times.append(t)

    log = 'mysql_db.select_universal: indexed={}, count= {}, time_mean={}'
    log_res(log.format(indexed, count, statistics.mean(times)), times)

def test_mysql_db_select_normalized(indexed, iterations):
    mysql_db.bulk_insert_normalized(path=PATH, indexed=indexed)
    times = []

    for i in range(iterations):
        t, count = mysql_db.select_normalized(indexed=indexed, path=PATH)
        times.append(t)

    log = 'mysql_db.select_normalized: indexed={}, count= {}, time_mean={}'
    log_res(log.format(indexed, count, statistics.mean(times)), times)


def test_mongo_db_scan(indexed, iterations):
    # repopulate database
    mongo_db.bulk_insert(path=PATH, indexed=False, drop_on_start=True, drop_on_exit=False)

    # wait until previous operation is complete
    time.sleep(0.5)

    times = []

    for i in range(iterations):
        t, scanned = mongo_db.scan()
        times.append(t)

    log = 'mongo_db.scan: db_size={}, scanned={}, time_mean={}'
    db_size = "{}MB".format(round(os.path.getsize(PATH) / 1024 / 1024, 2))
    log_res(log.format(db_size, scanned, statistics.mean(times)), times)
def test_mongo_db_scan_collections(indexed, iterations):
    # repopulate database
    mongo_db.bulk_insert_collections(path=PATH, indexed=False, drop_on_start=True, drop_on_exit=False)

    # wait until previous operation is complete
    time.sleep(0.5)

    times = []

    for i in range(iterations):
        t, scanned = mongo_db.scan_collections()
        times.append(t)

    log = 'mongo_db.scan_collections: db_size={}, scanned={}, time_mean={}'
    db_size = "{}MB".format(round(os.path.getsize(PATH) / 1024 / 1024, 2))
    log_res(log.format(db_size, scanned, statistics.mean(times)), times)
def test_mysql_db_scan_universal(indexed, iterations):
    # repopulate database
    mysql_db.bulk_insert_universal(path=PATH, indexed=False)

    # wait until previous operation is complete
    time.sleep(0.5)

    times = []

    for i in range(iterations):
        t, scanned = mysql_db.scan_universal()
        times.append(t)

    log = 'mysql_db.scan_universal: db_size={}, scanned={}, time_mean={}'
    db_size = "{}MB".format(round(os.path.getsize(PATH) / 1024 / 1024, 2))
    log_res(log.format(db_size, scanned, statistics.mean(times)), times)
def test_mysql_db_scan_normalized(indexed, iterations):
    # repopulate database
    mysql_db.bulk_insert_normalized(path=PATH, indexed=False)

    # wait until previous operation is complete
    time.sleep(0.5)

    times = []

    for i in range(iterations):
        t, scanned = mysql_db.scan_normalized()
        times.append(t)

    log = 'mysql_db.scan_normalized: db_size={}, scanned={}, time_mean={}'
    db_size = "{}MB".format(round(os.path.getsize(PATH) / 1024 / 1024, 2))
    log_res(log.format(db_size, scanned, statistics.mean(times)), times)



############################################################
#              MANUAL TEST PROCEDURES END                  #
############################################################


def parse():
    parser = argparse.ArgumentParser(description='DBD - Database Benchmark Driver')

    parser.add_argument('-t', '--test', help='Select a test to perform.', required=False, type=int,
                        choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14])

    parser.add_argument('-s', '--size', help='Select test data size.', required=False, type=int,
                        choices=[5, 50, 100, 500, 1000, 2000])

    parser.add_argument('-i', '--iterations', help='Number of test iterations to perform', type=int, required=False,
                        default=10)

    parser.add_argument('-si', '--simulated', help='Run database in simulated threaded environment', type=bool,
                        required=False, default=False)

    parser.add_argument('-th', '--threads', help='Number of threads for simulated envrionment', type=int,
                        required=False, default=3)

    parser.add_argument('-d', '--debug', help='Debugger verbosity.', required=False, default='v',
                        choices=['v', 'vv', 'vvv'])

    return parser.parse_args()


def start_threads(id, stop=False, database=1):
    while True:
        logging.info('Thread-{} started'.format(id))
        if database == 'MongoDB':
            mongo_db.simulation(write_concern=0)
        if database == 'MySQL':
            mysql_db.simulation()
        if stop():
            logging.info("Stopping Thread-{}".format(id))
            break
    logging.info("Thread-{} stopped".format(id))


def run_test(database, target, threads, indexed=False, iterations=ITERATIONS, simulated=False):

    exit_flag = not simulated

    workers = []

    if not exit_flag:
        for id in range(threads):
            tmp = threading.Thread(target=start_threads, args=(id, lambda: exit_flag, database))
            workers.append(tmp)
            tmp.daemon = True
            tmp.start()

    # time.sleep(2)

    t = threading.Thread(target=target, args=(indexed, iterations))

    t.start()
    t.join()

    exit_flag = True

    for worker in workers:
        worker.join()


if __name__ == "__main__":
    args = parse()

    # set database: 1=MongoDB, 2=MySQL
    database = ['MongoDB', 'MySQL']
    # set target test:
    test = [
        test_mongo_db_bulk_insert,                  #0
        test_mongo_db_bulk_insert_collections,      #1
        test_mysql_db_bulk_insert_universal,        #2
        test_mysql_db_bulk_insert_normalized,       #3

        test_mongo_db_bulk_insert_one,              #4
        test_mongo_db_bulk_insert_one_collections,  #5
        test_mysql_db_bulk_insert_one_universal,    #6
        test_mysql_db_bulk_insert_one_normalized,   #7

        test_mongo_db_insert_one,                   #8
        test_mongo_db_insert_one_collections,       #9
        test_mysql_db_insert_one_universal,         #10
        test_mysql_db_insert_one_normalized,        #11

        test_mongo_db_find,                         #12
        test_mongo_db_find_collections,             #13
        test_mysql_db_select_universal,             #14
        test_mysql_db_select_normalized,            #15

        test_mongo_db_scan,                         #16
        test_mongo_db_scan_collections,             #17
        test_mysql_db_scan_universal,               #18
        test_mysql_db_scan_normalized               #19
    ]

    # mysql_db.create_schema()

    #run_test(database=database[0], target=test[12], simulated=False, threads=3, iterations=10, indexed=True)
    #run_test(database=database[0], target=test[13], simulated=False, threads=3, iterations=10, indexed=True)
    #run_test(database=database[0], target=test[14], simulated=False, threads=3, iterations=10, indexed=True)
    #run_test(database=database[0], target=test[15], simulated=False, threads=3, iterations=10, indexed=True)

    #run_test(database=database[0], target=test[16], simulated=False, threads=3, iterations=10, indexed=False)
    #run_test(database=database[0], target=test[17], simulated=False, threads=3, iterations=10, indexed=False)

    run_test(database=database[0], target=test[4], simulated=False, threads=3, iterations=2, indexed=False)
    run_test(database=database[0], target=test[5], simulated=False, threads=3, iterations=2, indexed=False)
    run_test(database=database[0], target=test[6], simulated=False, threads=3, iterations=2, indexed=False)
    run_test(database=database[0], target=test[7], simulated=False, threads=3, iterations=2, indexed=False)

