# -*- coding: utf-8 -*-
"""
MongoDB Database Driver

    This class contains all the accessor methods for manipulating MongoDB databases.

    For a single server, the default host is assigned to: localhost, port 27017. For further adjustments to the
    database connector please see config.py.
"""

import json
import time
import logging
import pymongo
from pymongo import MongoClient
from pymongo import errors
import os
import config

logger = logging.getLogger(__name__)

# set defaults
DATABASE = config.database
DATABASE_INDEXED = config.indexed_database
COLLECTION = config.collection
DOCUMENT = config.document
DOCUMENT_DICT = config.document_dict
DOCUMENT_SINGLE = config.document_single
DATABASE_COLLECTION = config.collection_database
HOST = config.host
PORT = config.port


def connect(host, port):
    """Connect to the MongoDB Server on host:port

    Parameters:
        host - see configs - [default = 'localhost']
        port - see configs - [default = 27017]
    Returns:
        client - MongoClient object"""

    global client
    try:
        client = MongoClient(host, port)
        logger.debug("CONNECTED ON: {}:{}".format(client.HOST, client.PORT))
    except pymongo.errors.ConnectionFailure as code:
        logger.warning("PyMongo Error: {}".format(code))
    except pymongo.errors.PyMongoError as code:
        logger.warning("PyMongo Error: {}".format(code))
    return client


def create_indexes(database):
    """Create indexes after dropping 'benchmark_db_indexed'

    Parameters:
        database - the database where indexes will be created"""

    try:
        coll = database.get_collection(COLLECTION)
        coll.create_index([("id", pymongo.ASCENDING)], name='tweet_id_index')
        coll.create_index([("user.id", pymongo.ASCENDING)], name='user.id_index')
        coll.create_index([("user.followers_count", pymongo.ASCENDING)], name='user.follower_count_index')
        coll.create_index([("user.friends_count", pymongo.ASCENDING)], name='user.friends_count_index')
        coll.create_index([("location", pymongo.ASCENDING)], name='location_index')
    except pymongo.errors.CollectionInvalid as code:
        logger.warning("PyMongo Error: {}".format(code))
    except pymongo.errors.PyMongoError as code:
        logger.warning("PyMongo Error: {}".format(code))


def insert_one(indexed, doc_path, drop_on_start, drop_on_exit=False):
    """Inserts a single document to the benchmark_db database

       Parameters:
           indexed          - insert with indexes
           doc_path         - database document path
           drop_on_start    - drop database before query
           drop_on_exit     - drop database after query

       Returns:
           insert_one_time  - execution time for one insert
           bulk_insert_time - execution time for bulk insert
           doc_size         - size of the inserted document
           db_size          - size of the database"""

    if indexed:
        database = DATABASE_INDEXED
    else:
        database = DATABASE

    if drop_on_start: drop_database(database)

    db = connect(HOST, PORT).get_database(database)
    coll = db.get_collection(COLLECTION)

    d1 = open(doc_path, 'r')
    bulk_doc = json.load(d1)

    d2 = open(DOCUMENT_SINGLE, 'r')
    single_doc = json.load(d2)

    start = time.time()
    coll.insert_many(bulk_doc)
    bulk_insert_time = time.time() - start

    start = time.time()
    coll.insert_one(single_doc)
    insert_one_time = time.time() - start

    doc_size = "{}MB".format(round(os.path.getsize(DOCUMENT_SINGLE) / 1024 / 1024, 2))
    db_size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))

    logger.info("{} seconds to insert one indexed={} db_size={} doc_size={}".format(insert_one_time, indexed, db_size,
                                                                                    doc_size))

    if drop_on_exit: drop_database(database)

    return insert_one_time, doc_size, db_size, bulk_insert_time


def bulk_insert(indexed, doc_path, drop_on_start, drop_on_exit=False):
    """Bulk insert into MongoDB database

    Parameters:
        indexed - insert into benchmark_db_indexed [default = False]

    Returns:

    """

    # check drop flag
    if drop_on_start: drop_database(DATABASE)

    db = connect(HOST, PORT).get_database(DATABASE)
    coll = db.get_collection(COLLECTION, write_concern=pymongo.WriteConcern(w=0))

    document = open(doc_path, 'r')
    document = json.load(document)

    start = time.time()
    coll.insert_many(document)
    run = time.time() - start

    if indexed:
        db2 = connect(HOST, PORT).get_database(DATABASE_INDEXED)
        coll2 = db2.get_collection(COLLECTION)
        drop_database(DATABASE_INDEXED)
        create_indexes(db2)
        coll2.insert_many(document)

    count = coll.count()
    size = "{}MB".format(round(os.path.getsize(doc_path) / 1024 / 1024, 2))
    logger.info("{} seconds to bulk insert {} indexed={}".format(run, size, indexed))

    # check drop flag on exit
    if drop_on_exit: drop_database(DATABASE)

    return run, size


def find(indexed, doc_path):
    client = MongoClient(HOST, PORT)

    if indexed:
        db = client.get_database(DATABASE_INDEXED)
        # bulk_insert(doc_path=doc_path,indexed=True)
        coll = db.get_collection(COLLECTION)

    else:
        db = client.get_database(DATABASE)
        # bulk_insert(doc_path=doc_path,indexed=False)
        coll = db.get_collection(COLLECTION)

    res = 0

    run = 0

    # start = time.time()
    for i in range(5):
        start = time.time()
        res = coll.find({'user.location': 'London'}).count()
        run += time.time() - start

    for i in range(5):
        start = time.time()

        res = coll.find({'user.friends_count': {'$gt': 1000}}).count()
        run += time.time() - start

    for i in range(5):
        start = time.time()

        res = coll.find({'user.followers_count': {'$gt': 1000}}).count()
        run += time.time() - start

    # run = time.time() - start

    count = coll.count()

    size = "{}MB".format(round(os.path.getsize(doc_path) / 1024 / 1024, 2))
    logger.info("{} seconds to find {} with indexed={}, doc_size={}".format(run, res, indexed, size))

    return run, size


def scan_all(doc_path):
    client = MongoClient(HOST, PORT)
    db = client.get_database(DATABASE)
    coll = db.get_collection(COLLECTION)

    start = time.time()
    coll.find({}).count()
    run = time.time() - start

    count = coll.count()

    size = "{}MB".format(round(os.path.getsize(doc_path) / 1024 / 1024, 2))
    logger.info("{} seconds to scan {} db_size={}".format(run, count, size))

    # logger.info("%.16f seconds to scan %d objects", run, count)
    return run, count


def drop_database(database):
    try:
        client = MongoClient(HOST, PORT)
        #connect(HOST, PORT).drop_database(database)
        db = client.get_database(database)
        coll=db.get_collection(COLLECTION)
        coll.remove({})
        # client.drop_database(database)
        logger.debug("DROPPED {}!".format(database))

    except pymongo.errors as e:
        logger.warning("DROP ERROR: " + e)


def bulk_insert_collections(doc_path):
    drop_database(DATABASE_COLLECTION)

    client = MongoClient(HOST, PORT)
    db = client.get_database(DATABASE_COLLECTION)
    coll_users = db.get_collection('coll_users')
    coll_tweets = db.get_collection('coll_tweets')

    document = open(DOCUMENT_DICT, 'r')
    document = json.load(document)

    # users = []
    # tweets = []

    exec = 0

    coll_tweets.create_index([("id", pymongo.ASCENDING)], name='tweet.id index', unique=True)
    coll_users.create_index([("id", pymongo.ASCENDING)], name='user.id index', unique=True)

    doc1 = open('../dump1.json', 'w')
    doc2 = open('../dump2.json', 'w')
    doc1.write('['), doc2.write('[')

    count = 0

    for doc in document:
        doc['user_id'] = doc['user']['id']
        del doc['user']

        if count == (len(document) - 1):
            doc1.write(json.dumps(doc))
        else:
            doc1.write(json.dumps(doc))
            doc1.write(',\n')
            count += 1
        # start = time.time()
        # coll_tweets.insert_one(doc)
        # exec += time.time() - start

    document = open(DOCUMENT_DICT, 'r')
    document = json.load(document)
    count = 0

    for doc in document:
        if count == (len(document) - 1):
            doc2.write(json.dumps(doc['user']))
        else:
            doc2.write(json.dumps(doc['user']))
            doc2.write(',\n')
            count += 1

        # start = time.time()
        # coll_users.insert_one(doc['user'])
        # exec += time.time() - start

    doc1.write(']'), doc2.write(']')
    doc1.close(), doc2.close()

    document = open('../dump1.json', 'r')
    document = json.load(document)

    start = time.time()
    try:
        coll_tweets.insert_many(document, ordered=False)
    except Exception as e:
        # print(e)
        pass
    exec += time.time() - start

    document = open('../dump2.json', 'r')
    document = json.load(document)

    start = time.time()
    try:
        coll_users.insert_many(document, ordered=False)
    except Exception as e:
        # print(e)
        pass
    exec += time.time() - start

    size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))
    logger.info("{} seconds to bulk insert into collections {}".format(exec, size))

    return exec, size


# not used in benchmarks
def bulk_insert_one(doc_path, indexed=False):
    """Bulk insert one into MongoDB database


    """

    # drop database
    drop_database(DATABASE)

    db = connect().get_database(DATABASE)
    coll = db.get_collection(COLLECTION)

    document = open(DOCUMENT_DICT, 'r')
    document = json.load(document)

    start = time.time()
    for doc in document:
        coll.insert_one(doc)
    run = time.time() - start

    if indexed:
        db2 = connect().get_database(DATABASE_INDEXED)
        coll2 = db2.get_collection(COLLECTION)
        drop_database(DATABASE_INDEXED)
        create_indexes(db2)
        coll2.insert_many(document)

    count = coll.count()
    size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))
    logger.info("{} seconds to bulk insert {}".format(run, size))

    return run, size