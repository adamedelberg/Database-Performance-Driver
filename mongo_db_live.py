# -*- coding: utf-8 -*-
"""
MongoDB live database simulator
author: Adam Edelberg
date: 13 Aug 2018
"""

import threading
import json

import time
from pymongo import MongoClient

HOST = 'localhost'
PORT = 27017
DATABASE = 'benchmark_db'
COLLECTION = 'benchmark_coll'


class MyThread (threading.Thread):
   def __init__(self, id, name):
      threading.Thread.__init__(self)
      self.id = id
      self.name = name

   def run(self):
      simulate_mongodb(self.name)

def simulate_mongodb(thread):
    client = MongoClient(host=HOST,port=PORT)
    database = client.get_database(DATABASE)
    collection = database.get_collection(COLLECTION)

    print('Simulate: ' + thread)

    bulk = collection.initialize_unordered_bulk_op()

    #time.sleep(10)

    bulk.find({'lang': 'en'}).update({'$set': {'test1': 'True'}})
    bulk.find({'lang': 'en'}).update({'$set': {'test2': 'True'}})
    bulk.find({'lang': 'en'}).update({'$set': {'test3': 'True'}})
    bulk.find({'lang': 'en'}).update({'$set': {'test4': 'True'}})
    bulk.find({'lang': 'en'}).update({'$set': {'test5': 'True'}})
    bulk.find({'lang': 'en'}).update({'$set': {'test6': 'True'}})
    bulk.find({'lang': 'en'}).update({'$set': {'test7': 'True'}})
    bulk.find({'lang': 'en'}).update({'$set': {'test8': 'True'}})
    bulk.find({'lang': 'en'}).update({'$set': {'test9': 'True'}})
    bulk.find({'lang': 'en'}).update({'$set': {'test10': 'True'}})

    bulk.find({'lang': 'en'}).remove_one()
    bulk.find({'lang': 'en'}).remove_one()
    bulk.find({'lang': 'en'}).remove_one()
    bulk.find({'lang': 'en'}).remove_one()
    bulk.find({'lang': 'en'}).remove_one()
    bulk.find({'lang': 'en'}).remove_one()
    bulk.find({'lang': 'en'}).remove_one()
    bulk.find({'lang': 'en'}).remove_one()
    bulk.find({'lang': 'en'}).remove_one()
    bulk.find({'lang': 'en'}).remove_one()

    execute = bulk.execute()

    print(execute)

    #time.sleep(200)


def main(n):
    # Create new threads and start them

        for x in range (n):
            mythread = MyThread(x, "Thread-{}".format(x+1))
            mythread.start()
            mythread.join()


if __name__ == '__main__':
    main()