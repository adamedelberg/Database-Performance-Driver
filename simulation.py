import threading

import time

import mongo_db
import mysql_db


def start(database, threads):
    print("database: {}".format(database))
    print("threads: {}".format(threads))

    for i in range(threads):
        t = DatabaseThreads('Thread-{}'.format(i), 1)
        t.start()


class DatabaseThreads(threading.Thread):
    def __init__(self, name, database):
        threading.Thread.__init__(self)
        self.name = name
        self.database = database

    def run(self):

        # 1 = MongoDB, 2 = MySQL, 3 = Both

        if self.database == 1:
            mongo_db.simulation(write_concern=0)

        if self.database == 2:
            mysql_db.simulation()

        if self.database == 3:
            mongo_db.simulation(write_concern=0)
            mysql_db.simulation()

        time.sleep(0.1)
        # print(self.name)
