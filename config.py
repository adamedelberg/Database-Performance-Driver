# -*- coding: utf-8 -*-
"""
Configurations Options

    These settings will override all MongoDB and MySQL connections and operations spawned.

"""
import pymongo
import logging

# debugger level
DEBUGGING_LEVEL = logging.DEBUG
#DEBUGGING_LEVEL = logging.INFO


iterations = 10
threads = 10

#  MongoDB Configs
host = 'localhost'
port = 27017

shard_host = '137.158.59.81'
shard_port = 27016

#   MySQL Configs
mysql_host = 'localhost'
mysql_port = 3306

username = 'root'
password = 'passQ123'

# Indexes

indexes = [[('id', pymongo.ASCENDING)], [('user.id', pymongo.ASCENDING)]]

# Database

database = 'benchmark_db'
indexed_database = 'benchmark_db_indexed'
collection_database = 'benchmark_db_collection'
collection = 'benchmark_coll'

# Test data paths
document = '../parsed_data/e3-5MB.json'
#document = '../parsed_data/e3-50MB.json'
#document = '../parsed_data/e3-100MB.json'
#document = '../parsed_data/e3-500MB.json'
#document = '../parsed_data/e10-1GB.json'
#document = '../parsed_data/big-5GB.json'



document_dict = "{}-d.json".format(document[:-5])

# do not change unless necessary
document_single = '../parsed_data/single.json'