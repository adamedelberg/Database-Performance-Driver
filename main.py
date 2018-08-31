# -*- coding: utf-8 -*-
"""
Main driver
author: Adam Edelberg
date: 02 Aug 2018
"""

import csv
import logging

import argparse
import queue
import random
import threading

import time
import simulation
import mongo_db
import mysql_db

import statistics
import config
import os

ITERATIONS = config.iterations
THREADS = config.threads

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


############################################################
#                  MANUAL TEST PROCEDURES                  #
############################################################


def test_mongo_db_bulk_insert():
    # timing and metric variables
    times = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, doc_size = mongo_db.bulk_insert(path=PATH, indexed=False, drop_on_start=True)
        times.append(t)

    # log results
    log = 'mongo_db.bulk_insert: doc_size={}, time_mean={}'
    log(log.format(doc_size, statistics.mean(times)), times)


def test_mongo_db_bulk_insert_collections():
    times = []

    for i in range(ITERATIONS):
        t, doc_size = mongo_db.bulk_insert_collections(path=PATH, indexed=False, drop_on_start=True)
        times.append(t)

    log = 'mongo_db.bulk_insert_collections: doc_size={}, time_mean={}'
    log(log.format(doc_size, statistics.mean(times)), times)


def test_mysql_db_bulk_insert_universal():
    times = []

    for runs in range(ITERATIONS):
        t, size = mysql_db.bulk_insert_universal(path=PATH)
        times.append(t)

    log = 'mysql_db.bulk_insert_universal: doc_size={}, time_mean={}'
    log(log.format(size, statistics.mean(times)), times)


def test_mysql_db_bulk_insert_normalized():
    times = []

    for runs in range(ITERATIONS):
        t, doc_size = mysql_db.bulk_insert_normalized(path=PATH)
        times.append(t)

    log = 'mysql_db.bulk_insert_normalized: doc_size={}, time_mean={}'
    log(log.format(doc_size, statistics.mean(times)), times)


def test_mongo_db_bulk_insert_one():
    # timing and metric variables
    times = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, doc_size = mongo_db.bulk_insert_one(path=PATH, drop_on_start=True)
        times.append(t)

    # log results
    log = 'mongo_db.bulk_insert_one: doc_size={}, time_mean={}'
    log(log.format(doc_size, statistics.mean(times)), times)


def test_mysql_db_bulk_insert_one():
    # timing and metric variables
    times = []

    # perform multiple test iterations
    for i in range(ITERATIONS):
        t, doc_size = mysql_db.bulk_insert_one(path=PATH, indexed=False)
        times.append(t)

    # log results
    log = 'mysql_db.bulk_insert_one: doc_size={}, time_mean={}'
    log(log.format(doc_size, statistics.mean(times)), times)


def test_mongo_db_insert_one(indexed):
    times, bulk = [], []

    for i in range(ITERATIONS):
        t, db_size, doc_size, bulk_insert_time = mongo_db.insert_one(indexed=indexed, drop_on_start=True,
                                                                     path=PATH)
        times.append(t), bulk.append(bulk_insert_time)

    log = 'mongo_db.insert_one: indexed={}, db_size= {}, doc_size={}, time_mean={}, insert_time={}'
    log(log.format(indexed, doc_size, db_size, statistics.mean(times), statistics.mean(bulk)), times)


def test_mysql_db_insert_one(indexed):
    times, bulk = [], []

    for runs in range(ITERATIONS):
        t, db_size, doc_size, run = mysql_db.insert_one(indexed)
        times.append(t), bulk.append(run)

    log = 'mysql_db.universal_insert_one: indexed={}, db_size= {}, doc_size={}, time_mean={}, insert_time={}'
    log(log.format(indexed, doc_size, db_size, statistics.mean(times), statistics.mean(bulk)), times)


def test_mongo_db_find(indexed):
    mongo_db.bulk_insert(path=PATH, indexed=indexed, drop_on_start=True, drop_on_exit=False)

    times = []

    for i in range(ITERATIONS):
        t, count = mongo_db.find(indexed=indexed)
        times.append(t)

    log = 'mongo_db.find: indexed={}, count= {}, time_mean={}'
    log(log.format(indexed, count, statistics.mean(times)), times)


def test_mysql_db_select(indexed):
    mysql_db.bulk_insert_universal(path=PATH, indexed=indexed)
    times = []

    for i in range(ITERATIONS):
        t, db_size = mysql_db.select(indexed=indexed, path=PATH)
        times.append(t)

    log = 'mysql_db.select: indexed={}, db_size= {}, time_mean={}'
    log(log.format(indexed, db_size, statistics.mean(times)), times)


def test_mongo_db_scan():
    # repopulate database
    mongo_db.bulk_insert(path=PATH, indexed=False, drop_on_start=True, drop_on_exit=False)

    # wait until previous operation is complete
    time.sleep(0.5)

    times = []

    for i in range(ITERATIONS):
        t, scanned = mongo_db.scan()
        times.append(t)

    log = 'mongo_db.scan: db_size={}, scanned={}, time_mean={}'
    db_size = "{}MB".format(round(os.path.getsize(PATH) / 1024 / 1024, 2))
    log(log.format(db_size, scanned, statistics.mean(times)), times)


def test_mysql_db_scan():
    # repopulate database
    mysql_db.bulk_insert_universal(path=PATH, indexed=False)

    # wait until previous operation is complete
    time.sleep(0.5)

    times = []

    for i in range(ITERATIONS):
        t, scanned = mysql_db.scan()
        times.append(t)

    log = 'mysql_db.scan: db_size={}, scanned={}, time_mean={}'
    db_size = "{}MB".format(round(os.path.getsize(PATH) / 1024 / 1024, 2))
    log(log.format(db_size, scanned, statistics.mean(times)), times)


############################################################
#              MANUAL TEST PROCEDURES END                  #
############################################################


def log(tag, data):
    """

    :param tag:
    :param data:
    :return:
    """
    # print test data to console
    print(tag)
    try:
        # logs get appended to the same file
        with open(r'logs.csv', 'a') as report:
            writer = csv.writer(report, dialect='excel')
            writer.writerow([tag] + data)
            report.close()
    except csv.Error as code:
        logger.info("Logger: {}".format(code))


def parse():
    parser = argparse.ArgumentParser(description='DBD - Database Benchmark Driver')

    parser.add_argument('-t', '--test', help='Select a test to perform.', required=False, type=int,
                        choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14])

    parser.add_argument('-s', '--size', help='Select test data size.', required=False, type=int,
                        choices=[5, 50, 100, 500, 1000, 2000])

    parser.add_argument('-i', '--iterations', help='Number of test iterations to perform', type=int, required=False,
                        default=10)

    parser.add_argument('-d', '--debug', help='Debugger verbosity.', required=False, default='v',
                        choices=['v', 'vv', 'vvv'])

    return parser.parse_args()


# def func(id, result_queue):
#     while exit_flag:
#         print("Thread", id)
#         time.sleep(random.random() * 2)
#         result_queue.put((id, 'done'))
#
# def main():
#     q = queue.Queue()
#     threads = [ threading.Thread(target=func, args=(i, q)) for i in range(THREADS)]
#
#     for th in threads:
#         th.daemon = True
#         th.start()
#
#     result1 = q.get()
#     result2 = q.get()
#
#     print("Second result: {}".format(result1))
#     print("Second result: {}".format(result2))


def start_threads(id, stop=False, database=1):
    while True:
        logging.info('Thread-{}'.format(id))
        if database == 1:
            mongo_db.simulation(write_concern=0)
        if database == 2:
            mysql_db.simulation()
        if stop():
            print("Stopping threads...")
            break
    print("Thread-{} stopped".format(id))


def simulate(database):
    exit_flag = False

    workers = []

    for id in range(THREADS):
        tmp = threading.Thread(target=start_threads, args=(id, lambda: exit_flag, database))
        workers.append(tmp)
        tmp.daemon = True
        tmp.start()


    time.sleep(2)
    # alert to stop here
    exit_flag = True

    for worker in workers:
        worker.join()


if __name__ == "__main__":
    simulate(database=1)


    args = parse()

    # if args.test == 1: test_mongo_db_bulk_insert()
    # if args.test == 2: test_mongo_db_insert_one(indexed=True)
    # if args.test == 3: test_mongo_db_insert_one(indexed=False)
    # if args.test == 4: test_mongo_db_find(indexed=True)
    # if args.test == 5: test_mongo_db_find(indexed=False)
    # if args.test == 6: test_mongo_db_scan()
    # if args.test == 7: test_mysql_db_bulk_insert_universal()
    # if args.test == 8: test_mysql_db_bulk_insert_normalized()
    # if args.test == 9: test_mysql_db_insert_one(indexed=True)
    # if args.test == 10: test_mysql_db_insert_one(indexed=False)
    # if args.test == 11: test_mysql_db_select(indexed=True)
    # if args.test == 12: test_mysql_db_select(indexed=False)
    # if args.test == 13: test_mysql_db_scan()
    # if args.test == 14: test_mongo_db_bulk_insert_collections()

    # test_mongo_db_bulk_insert()
    # test_mongo_db_bulk_insert_collections()
    # test_mysql_db_bulk_insert_universal()
    # test_mysql_db_bulk_insert_normalized()

    # test_mongo_db_bulk_insert_one()
    # test_mysql_db_bulk_insert_one()

    # test_mongo_db_insert_one(indexed=True)
    # test_mongo_db_insert_one(indexed=False)
    # test_mysql_db_insert_one(indexed=True)
    # test_mysql_db_insert_one(indexed=False)

    # test_mongo_db_find(indexed=True)
    # test_mongo_db_find(indexed=False)
    # test_mysql_db_select(indexed=True)
    # test_mysql_db_select(indexed=False)

    # test_mongo_db_scan()
    # test_mysql_db_scan()

    # 1 = MongoDB, 2 = MySQL, 3 = Both
    # simulation.start(database=1, threads=5)
    #     exit_flag = False

    # simulation.DatabaseThreads.run()
    # while exit_flag:
    #    for i in range(THREADS):
    ##        t = simulation.DatabaseThreads('Thread-{}'.format(i), database=1)
    #       t.isDaemon()
    #       t.run()
