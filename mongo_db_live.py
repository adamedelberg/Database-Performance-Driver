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

    d1 = json.load(open('dump/1s.json'))
    d2 = json.load(open('dump/2s.json'))
    d3 = json.load(open('dump/3s.json'))
    d4 = json.load(open('dump/4s.json'))
    d5 = json.load(open('dump/5s.json'))
    d6 = json.load(open('dump/6s.json'))
    d7 = json.load(open('dump/7s.json'))
    d8 = json.load(open('dump/8s.json'))
    d9 = json.load(open('dump/9s.json'))
    d10 = json.load(open('dump/10s.json'))

    print('Simulate: ' + thread)

    bulk = collection.initialize_unordered_bulk_op()

    bulk.insert(d1)
    bulk.insert(d2)
    bulk.insert(d3)
    bulk.insert(d4)
    bulk.insert(d5)
    bulk.insert(d6)
    bulk.insert(d7)
    bulk.insert(d8)
    bulk.insert(d9)
    bulk.insert(d10)

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