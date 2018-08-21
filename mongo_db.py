# -*- coding: utf-8 -*-
"""
MongoDB Database Driver

    description

"""

import json
import time
import logging
import pymongo
from pymongo import MongoClient
import os

import config

logger = logging.getLogger(__name__)

HOST = config.host
PORT = config.port

DATABASE = config.database
DATABASE_INDEXED = config.indexed_database
COLLECTION = config.collection
DOCUMENT = config.document
DOCUMENT_DICT = config.document_dict
DOCUMENT_SINGLE = config.document_single


def connect(host=HOST, port=PORT):
    """Connect to the MongoDB Server

    Parameters:
        host - server host [default = 'localhost']
        port - server port [default = 27017]
    Returns:
        client - MongoClient object"""

    global client
    try:
        client = MongoClient(host, port)
        logger.debug("CONNECTED ON: {}:{}".format(client.HOST, client.PORT))
    except pymongo.errors.ConnectionFailure as err:
        logger.warning("CONNECTION FAILED! ERROR: {}".format(err))
    return client


def create_indexes(db):
    coll = db.get_collection(COLLECTION)

    coll.drop_indexes()

    coll.create_index([("id", pymongo.ASCENDING)], name='tweet_id_index')
    coll.create_index([("user.id", pymongo.ASCENDING)], name='user.id_index')
    coll.create_index([("user.followers_count", pymongo.ASCENDING)], name='user.follower_count_index')
    coll.create_index([("user.friends_count", pymongo.ASCENDING)], name='user.friends_count_index')
    coll.create_index([("location", pymongo.ASCENDING)], name='location_index')



def insert_one_indexed(drop=True):
    """Inserts a single document to the indexed benchmark_db database
    Parameters:
    Returns:
    """

    if drop: drop_database('benchmark_db_indexed')

    db = connect().get_database(DATABASE_INDEXED)

    coll = db.get_collection(COLLECTION)

    # create indexes

    create_indexes(db)

    document = open(DOCUMENT_DICT, 'r')
    document = json.load(document)
    start = time.time()

    coll.insert_many(document)
    run = time.time() - start

    document = open(DOCUMENT_SINGLE, 'r')
    document = json.load(document)

    # only measuring time to insert one with indexing

    start = time.time()
    coll.insert_one(document)
    run2 = time.time() - start

    size = "{}MB".format(round(os.path.getsize(DOCUMENT_SINGLE)/1024/1024, 2))
    logger.info("{} seconds to insert one with indexing {}".format(run, size))

    return run2, size


def insert_one_non_indexed(drop=True):
    if drop: drop_database(DATABASE)

    db = connect().get_database(DATABASE)

    coll = db.get_collection(COLLECTION)

    document = open(DOCUMENT_DICT, 'r')
    document = json.load(document)

    #coll.drop_indexes()
    start = time.time()

    coll.insert_many(document)
    run = time.time() - start

    document = open(DOCUMENT_SINGLE, 'r')
    document = json.load(document)

    # only measuring time to insert one without indexing

    start = time.time()
    coll.insert_one(document)
    run2 = time.time() - start

    size = "{}MB".format(round(os.path.getsize(DOCUMENT_SINGLE)/1024/1024, 2))
    logger.info("{} seconds to insert one with indexing {}".format(run, size))

    return run2,size


def bulk_insert(indexed=False):
    """Bulk insert into MongoDB database

    Parameters:
        indexed - insert into benchmark_db_indexed [default = False]

    Returns:

    """

    # drop database
    drop_database(DATABASE)

    db = connect().get_database(DATABASE)
    coll = db.get_collection(COLLECTION)

    document = open(DOCUMENT_DICT, 'r')
    document = json.load(document)

    start = time.time()
    coll.insert_many(document)
    run = time.time() - start

    if indexed:
        db2 = connect().get_database(DATABASE_INDEXED)
        coll2 = db2.get_collection(COLLECTION)
        drop_database(DATABASE_INDEXED)
        create_indexes(db2)
        coll2.insert_many(document)

    count = coll.count()
    size = "{}MB".format(round(os.path.getsize(DOCUMENT)/1024/1024, 2))
    logger.info("{} seconds to bulk insert {}".format(run, size))

    return run, size


def find(indexed):
    client = MongoClient(HOST, PORT)

    if indexed:
        db = client.get_database(DATABASE_INDEXED)
    else:
        db = client.get_database(DATABASE)

    coll = db.get_collection(COLLECTION)

    res = 0

    start = time.time()
    for i in range(5): res += coll.find({'id': {"$gt": 995403410492060000}}).count()
    for i in range(5): res += coll.find({'user.id': {"$gt": 995403410492060000}}).count()
    run = time.time() - start

    count = coll.count()
    #for x in coll.find():
    #    print(x)
    #print(coll.find({'id': {"$regex": '995'}}).count())

    logger.info("%.16f seconds to find %d with indexed= %s", run, res,indexed)
    return run


def scan_all():
    client = MongoClient(HOST, PORT)
    db = client.get_database(DATABASE)
    coll = db.get_collection(COLLECTION)

    start = time.time()
    coll.find({})
    run = time.time() - start

    count = coll.count()
    logger.info("%.16f seconds to scan %d objects", run, count)
    return run


def drop_database(database):
    try:
        #client = MongoClient(HOST, PORT)
        connect().drop_database(database)
        #client.drop_database(database)
        logger.debug("DROPPED {}!".format(database))

    except pymongo.errors as e:
        logger.warning("DROP ERROR: " + e)


def drop_databases():
    try:
        client = MongoClient(HOST, PORT)
        client.drop_database(DATABASE)
        logger.debug("DROPPED " + DATABASE + "!")
        client.drop_database(DATABASE_INDEXED)
        logger.debug("DROPPED " + DATABASE_INDEXED + "!")

    except pymongo.errors as e:
        logger.warning("DROP ERROR: " + e)


######################################################
#  DEPRECATED FUNCTIONS - DO NOT USE IN BENCHMARKS!  #
######################################################


def insert_one():
    drop_database()

    client = MongoClient(HOST, PORT)
    db = client.get_database(DATABASE)
    coll = db.get_collection(COLLECTION)

    document = open(DOCUMENT, 'r')

    start = time.time()

    with document as json_docs:
        for data in json_docs:
            data = json.loads(data)
            coll.insert_one(data)

    run = time.time() - start

    count = coll.count()
    logger.info("%s seconds to insertOne()  %d objects", run, count)
    return run


def indexed_find():

    client = MongoClient(HOST, PORT)
    db = client.get_database(DATABASE_INDEXED)
    coll = db.get_collection(COLLECTION)

    start = time.time()
    #res = coll.find({"user.follower_count": 100})

    res = coll.find({'id':995403410492067840}).count()
    res += coll.find({'id':995403410492067840}).count()
    res += coll.find({'id':995403410492067840}).count()
    res += coll.find({'id':995403410492067840}).count()
    res += coll.find({'id':995403410492067840}).count()
    res += coll.find({'user.id':995403410492067840}).count()
    res += coll.find({'user.id':995403410492067840}).count()
    res += coll.find({'user.id':995403410492067840}).count()
    res += coll.find({'user.id':995403410492067840}).count()
    res += coll.find({'user.id':995403410492067840}).count()
    res += coll.find({'user.id':995403410492067840}).count()

    run = time.time() - start

    #count = coll.count()

    logger.info("%.16f seconds to find %d indexed objects", run, res)
    return run


def non_indexed_find():

    client = MongoClient(HOST, PORT)
    db = client.get_database(DATABASE)
    coll = db.get_collection(COLLECTION)

    start = time.time()

    res = coll.find({'id':995403410492067840}).count()
    res += coll.find({'id':995403410492067840}).count()
    res += coll.find({'id':995403410492067840}).count()
    res += coll.find({'id':995403410492067840}).count()
    res += coll.find({'id':995403410492067840}).count()
    res += coll.find({'user.id':995403410492067840}).count()
    res += coll.find({'user.id':995403410492067840}).count()
    res += coll.find({'user.id':995403410492067840}).count()
    res += coll.find({'user.id':995403410492067840}).count()
    res += coll.find({'user.id':995403410492067840}).count()
    res += coll.find({'user.id':995403410492067840}).count()

    run = time.time() - start

    count = coll.count()
    logger.info("%.16f seconds to find %s non-indexed objects", run,res)

    return run