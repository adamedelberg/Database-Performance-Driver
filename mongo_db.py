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
import random

logger = logging.getLogger(__name__)

# set defaults
DATABASE = config.database
DATABASE_INDEXED = config.indexed_database
COLLECTION = config.collection
DOCUMENT = config.document
DOCUMENT_SINGLE = config.single
DATABASE_COLLECTION = config.collection_database
HOST = config.host_mongo
PORT = config.port_mongo


def connect(host, port):
    """
    Connect to the MongoDB Server on <host>:<port>

    :param host: see configs - [default = 'localhost']
    :param port: see configs - [default = 27017]
    :return: MongoClient object
    """

    global client
    try:
        client = MongoClient(host, port)
        logger.debug("CONNECTED ON: {}:{}".format(client.HOST, client.PORT))
    except pymongo.errors.ConnectionFailure as code:
        logger.warning("PyMongo Error: {}".format(code))
    except pymongo.errors.PyMongoError as code:
        logger.warning("PyMongo Error: {}".format(code))
    return client


def drop_database(database):
    """
    Safe drop database using remove

    :param database:
    :return:
    """
    try:
        db = MongoClient(HOST, PORT).get_database(database)
        coll = db.get_collection(COLLECTION)

        coll.remove({})

        db = MongoClient(HOST, PORT).get_database(DATABASE_COLLECTION)
        users = db.get_collection('users')
        tweets = db.get_collection('tweets')

        users.remove({})
        tweets.remove({})

        logger.debug("DROPPED {}!".format(database))
        logger.debug("DROPPED {}!".format(DATABASE_COLLECTION))

    except pymongo.errors as code:
        logger.warning("MongoDB Error: {}".format(code))


def drop_database_collections(database):
    """
    Safe drop database using remove

    :param database:
    :return:
    """
    try:
        client = MongoClient(HOST, PORT)
        db = client.get_database(database)
        users = db.get_collection('users')
        tweets = db.get_collection('tweets')

        users.remove({})
        tweets.remove({})

        # connect(HOST, PORT).drop_database(database)
        # client.drop_database(database)
        logger.debug("DROPPED {}!".format(database))

    except pymongo.errors as code:
        logger.warning("MongoDB Error: {}".format(code))


def create_indexes():
    """
    Create indexes in given database

    :param database: the database where indexes will be created
    :return:
    """
    remove_indexes()

    try:

        client = MongoClient(HOST,PORT)
        db = client.get_database(DATABASE)
        coll = db.get_collection(COLLECTION)

        coll.create_index([("id", pymongo.ASCENDING)], name='tweetIdx')
        coll.create_index([("user.id", pymongo.ASCENDING)], name='user.idIdx')
        coll.create_index([("user.followers_count", pymongo.ASCENDING)], name='user.follower_countIdx')
        coll.create_index([("user.friends_count", pymongo.ASCENDING)], name='user.friends_countIdx')
        coll.create_index([("location", pymongo.ASCENDING)], name='locationIdx')

        client = MongoClient(HOST, PORT)
        db = client.get_database(DATABASE_COLLECTION)

        tweet_coll = db.get_collection('tweets')
        users_coll = db.get_collection('users')

        tweet_coll.create_index([("id", pymongo.ASCENDING)], name='tweetIdx')
        users_coll.create_index([("id", pymongo.ASCENDING)], name='userIdx')
        users_coll.create_index([("followers_count", pymongo.ASCENDING)], name='follower_countIdx')
        users_coll.create_index([("friends_count", pymongo.ASCENDING)], name='friends_countIdx')
        users_coll.create_index([("location", pymongo.ASCENDING)], name='locationIdx')

    except pymongo.errors.PyMongoError as code:
        logger.warning("PyMongo Error: {}".format(code))
        pass

#TODO test
def remove_indexes():
    """
    Removes indexes in given database

    :param database: the database where indexes will be created
    :return:
    """

    try:
        client = MongoClient(HOST, PORT)
        db = client.get_database(DATABASE)
        coll = db.get_collection(COLLECTION)

        coll.drop_index('tweetIdx')
        coll.drop_index('user.idIdx')
        coll.drop_index('user.follower_countIdx')
        coll.drop_index('user.friends_countIdx')
        coll.drop_index('locationIdx')

        db = client.get_database(DATABASE_COLLECTION)
        tweet_coll = db.get_collection('tweets')
        users_coll = db.get_collection('users')

        tweet_coll.drop_index('tweetIdx')
        users_coll.drop_index('userIdx')
        users_coll.drop_index('follower_countIdx')
        users_coll.drop_index('friends_countIdx')
        users_coll.drop_index('locationIdx')

    except pymongo.errors.PyMongoError as code:
        logger.debug("PyMongo Error: {}".format(code))
        pass


def bulk_insert(path, indexed, drop_on_start, drop_on_exit=False, write_concern=1):
    """
    Bulk insert into MongoDB database

    :param path:
    :param indexed: insert into benchmark_db_indexed [default = False]
    :param drop_on_start:
    :param drop_on_exit:
    :param write_concern:
    :return:
    """

    # check drop flag
    if drop_on_start: drop_database(DATABASE)

    if indexed: create_indexes()
    else: remove_indexes()

    # connect to correct database:
    db = connect(HOST, PORT).get_database(DATABASE)
    coll = db.get_collection(COLLECTION, write_concern=pymongo.WriteConcern(w=write_concern))

    document = open(path, 'r')

    docs = []

    for doc in document:
        docs.append(json.loads(doc))

    start = time.time()
    coll.insert_many(docs)
    run = time.time() - start

    if indexed:
        create_indexes()
    else:
        remove_indexes()

    size = "{}MB".format(round(os.path.getsize(path) / 1024 / 1024, 2))
    logger.info("{} seconds to bulk_insert {}, indexed={}".format(run, size, indexed))

    # check drop flag on exit
    if drop_on_exit: drop_database(DATABASE)

    return run, size


def bulk_insert_collections(path, indexed, drop_on_start, drop_on_exit=False, write_concern=1):
    """

    :param path:
    :param indexed:
    :param drop_on_start:
    :param drop_on_exit:
    :param write_concern:
    :return:
    """
    if drop_on_start: drop_database_collections(DATABASE_COLLECTION)

    db = connect(HOST, PORT).get_database(DATABASE_COLLECTION)
    user_collection = db.get_collection('users', write_concern=pymongo.WriteConcern(w=write_concern))
    tweet_collection = db.get_collection('tweets', write_concern=pymongo.WriteConcern(w=write_concern))

    if indexed: create_indexes()
    else: remove_indexes()

    execution_time = 0

    document = open(path, 'r')

    users = []
    tweets = []

    for doc in document:
        d = json.loads(doc)
        users.append(d['user'])
        # add the user id to the tweet collection
        d['user_id'] = d['user']['id']
        del d['user']
        tweets.append(d)

    start_time = time.time()

    user_collection.insert_many(users)
    tweet_collection.insert_many(tweets)

    execution_time += time.time() - start_time

    size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))
    logger.info("{} seconds to bulk_insert_collections {}".format(execution_time, size))

    if drop_on_exit: drop_database(DATABASE)

    return execution_time, size


def bulk_insert_one(path, drop_on_start, drop_on_exit=False, write_concern=1):
    """

    :param path:
    :param drop_on_start:
    :param drop_on_exit:
    :param write_concern:
    :return:
    """

    # drop database
    if drop_on_start: drop_database(DATABASE)

    db = connect(HOST, PORT).get_database(DATABASE)
    coll = db.get_collection(COLLECTION, write_concern=pymongo.WriteConcern(w=write_concern))

    document = open(path, 'r')

    start = time.time()
    for doc in document:
        coll.insert_one(json.loads(doc))
    run = time.time() - start

    size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))
    logger.info("{} seconds to bulk insert one {}".format(run, size))

    if drop_on_exit: drop_database(DATABASE)

    return run, size


def bulk_insert_one_collections(path, drop_on_start, drop_on_exit=False, write_concern=1):
    """

    :param path:
    :param drop_on_start:
    :param drop_on_exit:
    :param write_concern:
    :return:
    """

    if drop_on_start: drop_database_collections(DATABASE_COLLECTION)

    db = connect(HOST, PORT).get_database(DATABASE_COLLECTION)
    user_collection = db.get_collection('users', write_concern=pymongo.WriteConcern(w=write_concern))
    tweet_collection = db.get_collection('tweets', write_concern=pymongo.WriteConcern(w=write_concern))

    execution_time = 0

    document = open(path, 'r')

    users = []
    tweets = []

    for doc in document:
        d = json.loads(doc)
        users.append(d['user'])
        # add the user id to the tweet collection
        d['user_id'] = d['user']['id']
        del d['user']
        tweets.append(d)


    start_time = time.time()

    for u in users:
        user_collection.insert_one(u)
    for t in tweets:
        tweet_collection.insert_one(t)

    execution_time += time.time() - start_time

    size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))
    logger.info("{} seconds to bulk insert one into collections {}".format(execution_time, size))

    if drop_on_exit: drop_database(DATABASE)

    return execution_time, size


def insert_one(path, indexed, drop_on_start, drop_on_exit=False, write_concern=1):
    """
    Inserts a single document to the benchmark_db database

    :param path:
    :param indexed:
    :param drop_on_start:
    :param drop_on_exit:
    :param write_concern:
    :return:

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

    if indexed: create_indexes()
    else: remove_indexes()

    if drop_on_start: drop_database(DATABASE)

    db = connect(HOST, PORT).get_database(DATABASE)
    coll = db.get_collection(COLLECTION, write_concern=pymongo.WriteConcern(w=write_concern))

    d1 = open(path, 'r')
    docs = []

    for doc in d1:
        docs.append(json.loads(doc))

    d2 = open(DOCUMENT_SINGLE, 'r')
    single_doc = json.load(d2)

    start = time.time()
    coll.insert_many(docs)
    bulk_insert_time = time.time() - start

    start = time.time()
    coll.insert_one(single_doc)
    insert_one_time = time.time() - start

    doc_size = "{}MB".format(round(os.path.getsize(DOCUMENT_SINGLE) / 1024 / 1024, 2))
    db_size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))

    logger.info("{} seconds to insert one indexed={} db_size={} doc_size={}".format(insert_one_time, indexed, db_size,
                                                                                    doc_size))

    if drop_on_exit: drop_database(DATABASE)

    return insert_one_time, doc_size, db_size, bulk_insert_time


def insert_one_collections(path, indexed, drop_on_start, drop_on_exit=False, write_concern=1):
    """
    Inserts a single document to the benchmark_db database

    :param path:
    :param indexed:
    :param drop_on_start:
    :param drop_on_exit:
    :param write_concern:
    :return:

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

    if indexed: create_indexes()
    else: remove_indexes()

    if drop_on_start: drop_database_collections(DATABASE_COLLECTION)

    db = connect(HOST, PORT).get_database(DATABASE_COLLECTION)
    user_collection = db.get_collection('users', write_concern=pymongo.WriteConcern(w=write_concern))
    tweet_collection = db.get_collection('tweets', write_concern=pymongo.WriteConcern(w=write_concern))

    d1 = open(path, 'r')
    docs = []

    users = []
    tweets = []

    for doc in d1:
        d = json.loads(doc)
        users.append(d['user'])
        # add the user id to the tweet collection
        d['user_id'] = d['user']['id']
        del d['user']
        tweets.append(d)

    start = time.time()

    user_collection.insert_many(users)
    tweet_collection.insert_many(tweets)

    bulk_insert_time = time.time() - start

    d2 = open(DOCUMENT_SINGLE, 'r')

    for doc in d2:
        d = json.loads(doc)
        users.append(d['user'])
        # add the user id to the tweet collection
        d['user_id'] = d['user']['id']
        del d['user']
        tweets.append(d)

    start = time.time()

    user_collection.insert_one(users.pop())
    tweet_collection.insert_one(tweets.pop())

    insert_one_time = time.time() - start

    doc_size = "{}MB".format(round(os.path.getsize(DOCUMENT_SINGLE) / 1024 / 1024, 2))
    db_size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))

    logger.info("{} seconds to insert one collections indexed={} db_size={} doc_size={}".format(insert_one_time, indexed, db_size,
                                                                                    doc_size))

    if drop_on_exit: drop_database(DATABASE_COLLECTION)

    return insert_one_time, doc_size, db_size, bulk_insert_time


def find(indexed):
    """

    :param indexed:
    :return:
    """

    if indexed:
        create_indexes()
    else:
        remove_indexes()

    db = connect(HOST, PORT).get_database(DATABASE)
    collection = db.get_collection(COLLECTION)

    execution_time = 0

    for i in range(5):
        count = 0

        start_time = time.time()


        count+=collection.find({'$and' :[{'user.location': 'London'}, {'user.friends_count': {'$gt': 1000}}, {'user.followers_count': {'$gt': 1000}}]}).count()

        execution_time += time.time() - start_time

    logger.info("{} seconds to find {} with indexed={}".format(execution_time, count, indexed))

    return execution_time, count


def find_collections(indexed):
    """

    :param indexed:
    :return:
    """

    if indexed:
        create_indexes()

    db = connect(HOST, PORT).get_database(DATABASE_COLLECTION)

    user_collection = db.get_collection('users', write_concern=pymongo.WriteConcern())
    tweet_collection = db.get_collection('tweets', write_concern=pymongo.WriteConcern())

    execution_time = 0

    for i in range(5):
        count = 0

        start_time = time.time()

        #count += user_collection.find({'location': 'London'}).count()
        #count += user_collection.find({'friends_count': {'$gt': 1000}}).count()
        #count += user_collection.find({'followers_count': {'$gt': 1000}}).count()

        count +=user_collection.find({'$and' :[{'location': 'London'}, {'friends_count': {'$gt': 1000}}, {'followers_count': {'$gt': 1000}}]}).count()

        execution_time += time.time() - start_time

    logger.info("{} seconds to find_collections {} with indexed={}".format(execution_time, count, indexed))

    return execution_time, count


def scan():
    """

    :return:
    """
    db = connect(HOST, PORT).get_database(DATABASE)

    collection = db.get_collection(COLLECTION)

    start_time = time.time()

    collection.find({}).count()

    execution_time = time.time() - start_time

    scanned = collection.count()

    logger.info("{} seconds to scan {} objects".format(execution_time, scanned))

    return execution_time, scanned


def scan_collections():
    """

    :return:
    """
    db = connect(HOST, PORT).get_database(DATABASE_COLLECTION)

    users = db.get_collection('users')
    tweets = db.get_collection('tweets')
    start_time = time.time()

    users.find({}).count()

    tweets.find({}).count()

    tweets.aggregate([{'$lookup': {'from': "users", 'localField': "user_id", 'foreignField': "id", 'as': "ids"}}])

    execution_time = time.time() - start_time

    scanned = users.count()
    scanned+= tweets.count()

    logger.info("{} seconds to scan_collections {} objects".format(execution_time, scanned))

    return execution_time, scanned


def simulation(write_concern=0):
    db = connect(HOST, PORT).get_database(DATABASE)
    coll = db.get_collection(COLLECTION, write_concern=write_concern)

    find_queries = [{'lang': 'ru'}, {'lang': 'es'}, {'lang': 'en'}, {'truncated': 'true'}, {'truncated': 'false'}, ]
    update_queries = ['true', 'false']

    id = 0
    for i in range(len(find_queries)):
        # find
        random_find = find_queries[random.randrange(0, len(find_queries))]
        coll.find(random_find).count()

        # update
        random_update = update_queries[random.randrange(0, len(update_queries))]
        coll.update_one({"truncated": random_update}, {"$set": {"truncated": random_update}})

        # insert
        id += 1
        coll.insert({"created_at":"test","id":id,"id_str":"test", "text":"test","truncated":'false'})








