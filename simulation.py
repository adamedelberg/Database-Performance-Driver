import logging
import random
import threading

import time

import mongo_db
import mysql_db

logger = logging.getLogger(__name__)


def start(database, threads):

    for i in range(threads):
        t = DatabaseThreads('Thread-{}'.format(i), database=database)
        t.start()


class DatabaseThreads(threading.Thread):
    def __init__(self, name, database):
        threading.Thread.__init__(self)
        self.name = name
        self.database = database

    def run(self):

        # 1 = MongoDB, 2 = MySQL,

        exit_flag = False

        while not exit_flag:
            # wait a random time between 0 and 5 seconds
            time.sleep(random.randrange(0, 5))

            if self.database == 1:
                    mongo_db.simulation(write_concern=0)
                    logger.info('{} simulating mongo'.format(self.name))

            if self.database == 2:
                    mysql_db.simulation()
                    logger.info('{} simulating mysql'.format(self.name))
