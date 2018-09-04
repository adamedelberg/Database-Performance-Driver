# -*- coding: utf-8 -*-
"""
Configurations Options

    These settings will override all MongoDB and MySQL connections and operations spawned.

"""
import pymongo
import logging

# debugger level
levels = [logging.INFO, logging.DEBUG, logging.WARNING]
DEBUG_LEVEL = levels[0]

iterations = 5
threads = 4

#  MongoDB Configs
host_mongo = '137.158.59.83'
port_mongo = 27018

host_mongo = 'localhost'
port_mongo = 27017

#host = '137.158.59.83'
#port = 27020

#   MySQL Configs
host_mysql = 'localhost'
port_mysql = 3306

username_mysql = 'root'
password_mysql = 'passQ123'

# Database

database = 'benchmark_db'
indexed_database = 'benchmark_db_indexed'
collection_database = 'benchmark_db_collection'
collection = 'tweets'

# Test data paths
document = '../parsed_data/e3-5MB.json'
#document = '../parsed_data/e3-50MB.json'
#document = '../parsed_data/e3-100MB.json'
#document = '../parsed_data/e3-500MB.json'
#document = '../parsed_data/e10-1GB.json'
#document = '../parsed_data/big-5GB.json'

docs = [
    '../parsed_data/e3-5MB.json',
    '../parsed_data/e3-50MB.json',
    '../parsed_data/e3-100MB.json',
    '../parsed_data/e3-500MB.json',
    '../parsed_data/e10-1GB.json',
    '../parsed_data/big-5GB.json']


# do not change unless necessary
single = '../parsed_data/single.json'