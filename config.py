# -*- coding: utf-8 -*-
"""
Configurations Options

    These settings will override all MongoDB and MySQL connections and operations spawned.

"""
import pymongo
import logging

# debugger level
DEBUG_LEVEL = logging.INFO

default_iterations = 5
default_threads = 4

#  MongoDB Configs
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
document = '../parsed_data/5.json'
#document = '../parsed_data/50.json'
#document = '../parsed_data/100.json'
#document = '../parsed_data/500.json'
#document = '../parsed_data/1000.json'
#document = '../parsed_data/2000.json'


# do not change unless necessary
single = '../parsed_data/single.json'


# MySQL Schema Path
schema = 'schema.sql'