# -*- coding: utf-8 -*-
"""
MongoDB Database Driver

    This class contains all the accessor methods for manipulating MongoDB databases.
    The default host is assigned as: localhost, port 27017. These settings can be adjusted in config.py.

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
DATABASE_COLLECTION = config.collection_database


def connect(host=HOST, port=PORT):
    """Connect to the MongoDB Server

    Parameters:
        host - see configs - server host [default = 'localhost']
        port - see configs - server port [default = 27017]
    Returns:
        client - MongoClient object"""

    global client
    try:
        client = MongoClient(host, port)
        logger.debug("CONNECTED ON: {}:{}".format(client.HOST, client.PORT))
    except pymongo.errors.ConnectionFailure as err:
        logger.warning("CONNECTION FAILED! ERROR: {}".format(err))
    return client


def create_indexes(database):
    """Create indexes

    The following five indexes get dropped and recreated at each insert test where relevant.

    Parameters:
        db - the database where indexes will be created

    """
    try:
        coll = database.get_collection(COLLECTION)
    except Exception:
        logger.warning("ERROR ON COLLECTION! see: create_indexes()")
    try:
        coll.create_index([("id", pymongo.ASCENDING)], name='tweet_id_index')
        coll.create_index([("user.id", pymongo.ASCENDING)], name='user.id_index')
        coll.create_index([("user.followers_count", pymongo.ASCENDING)], name='user.follower_count_index')
        coll.create_index([("user.friends_count", pymongo.ASCENDING)], name='user.friends_count_index')
        coll.create_index([("location", pymongo.ASCENDING)], name='location_index')
    except Exception:
        logger.warning("ERROR ON INDEXES! see: create_indexes()")


def insert_one_indexed(drop):
    """Inserts a single document to the indexed benchmark_db database
    Parameters:
    Returns:
    """

    if drop: drop_database('benchmark_db_indexed')
    client = MongoClient(HOST, PORT)
    database = client.get_database('benchmark_db_indexed')
    coll = database.get_collection(COLLECTION)

    # create indexes
    create_indexes(database)

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

    size = "{}MB".format(round(os.path.getsize(DOCUMENT_SINGLE) / 1024 / 1024, 2))
    logger.info("{} seconds to insert one with indexing {}".format(run, size))

    return run2, size


def insert_one_non_indexed(drop=True):
    if drop: drop_database(DATABASE)

    db = connect().get_database(DATABASE)

    coll = db.get_collection(COLLECTION)

    document = open(DOCUMENT_DICT, 'r')
    document = json.load(document)

    # coll.drop_indexes()
    start = time.time()

    coll.insert_many(document)
    run = time.time() - start

    document = open(DOCUMENT_SINGLE, 'r')
    document = json.load(document)

    # only measuring time to insert one without indexing

    start = time.time()
    coll.insert_one(document)
    run2 = time.time() - start

    size = "{}MB".format(round(os.path.getsize(DOCUMENT_SINGLE) / 1024 / 1024, 2))
    logger.info("{} seconds to insert one with indexing {}".format(run, size))

    return run2, size


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
    size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))
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
    for i in range(5): res = coll.find({'user.location': 'London'}).count()
    for i in range(5): res = coll.find({'user.friends_count': {'$gt': 1000}}).count()
    for i in range(5): res = coll.find({'user.followers_count': {'$gt': 1000}}).count()

    run = time.time() - start

    count = coll.count()
    # for x in coll.find():
    #    print(x)
    # print(coll.find({'id': {"$regex": '995'}}).count())

    logger.info("%.16f seconds to find %d with indexed= %s", run, res, indexed)
    return run


def scan_all():
    client = MongoClient(HOST, PORT)
    db = client.get_database(DATABASE)
    coll = db.get_collection(COLLECTION)

    start = time.time()
    coll.find({}).count()
    run = time.time() - start

    count = coll.count()

    logger.info("%.16f seconds to scan %d objects", run, count)
    return run


def drop_database(database):
    try:
        # client = MongoClient(HOST, PORT)
        connect().drop_database(database)
        # client.drop_database(database)
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


def bulk_insert_collections():
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

    doc1 = open('../doc1.json', 'w')
    doc2 = open('../doc2.json', 'w')
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

    document = open('../doc1.json', 'r')
    document = json.load(document)

    start = time.time()
    try:
        coll_tweets.insert_many(document, ordered=False)
    except Exception:
        print('hi')
    exec += time.time() - start

    document = open('../doc2.json', 'r')
    document = json.load(document)

    start = time.time()
    try:
        coll_users.insert_many(document, ordered=False)
    except Exception:
        print()
    exec += time.time() - start

    size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))
    logger.info("{} seconds to bulk insert into collections {}".format(exec, size))

    return exec, size
