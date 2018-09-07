# -*- coding: utf-8 -*-
"""
MySQL Database Driver

    description
"""
import io
import json
import logging
import random
import time

import os
import pymysql
import mysql.connector
import re

import config

logger = logging.getLogger(__name__)

# set defaults
USER = config.username_mysql
PASS = config.password_mysql
HOST = config.host_mysql
PORT = config.port_mysql
DATABASE = config.database
DOCUMENT = config.document
DOCUMENT_SINGLE = config.single


def connect(host, port, user, password, database):
    """Connect to the MySQL Server

    Parameters:
        host        - server host      - [default = 'localhost']
        port        - server port      - [default = 3306]
        user        - username         - [default = 'root']
        password    - password         - [default = 'root']
        database    - database name    - [default = 'benchmark_db']
    Returns:
        client      - PyMySQL Connector Object"""

    global client
    try:
        client = pymysql.connect(user=user, password=password, host=host, db=database, port=port, autocommit=False)
        logger.debug("CONNECTED ON: {}:{}".format(client.host, client.port))
    except pymysql.err.Error as code:
        logger.info("PyMySQL Error: {}".format(code))
    return client


def create_schema(sql_file='schema.sql'):
    conn = pymysql.connect(user=USER, password=PASS, host=HOST, autocommit=False)
    cursor = conn.cursor()

    logger.info('Executing SQL script: {}'.format(sql_file))

    statement = ""

    for line in open(sql_file):
        if re.match(r'--', line):  # ignore sql comment lines
            continue
        if not re.search(r'[^-;]+;', line):  # keep appending lines that don't end in ';'
            statement = statement + line
        else:  # when you get a line ending in ';' then exec statement and reset for next statement
            statement = statement + line
            try:
                cursor.execute(statement)
            except Exception as e:
                #print ("\n[WARN] MySQLError during execute statement \n\tArgs: '%s'" % (str(e.args)))
                logger.info('MySQLError: {}'.format(e.args))
            statement = ""


def create_indexes():
    # remove any created indexes
    remove_indexes()

    conn = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    cursor = conn.cursor()

    sqls = ['CREATE INDEX tweet_index ON tweets (`id`);',
            'CREATE INDEX user_index ON users (`id`);',
            'CREATE INDEX follower_index ON users (`followers_count`);',
            'CREATE INDEX friend_index ON users (`friends_count`);',
            'CREATE INDEX indx ON universal (`tweets.id`,`users.id`,`users.followers_count`,`users.friends_count`);']

    try:
        for sql in sqls:
            cursor.execute(sql)
    except Exception as code:
        logger.debug('PyMySQL Error: {}'.format(code))
        pass

    cursor.close()
    conn.commit()
    conn.close()


def remove_indexes():
    conn = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    cursor = conn.cursor()
    sqls = ['DROP INDEX tweet_index ON tweets;',
            'DROP INDEX user_index ON users ;',
            'DROP INDEX follower_index ON users;',
            'DROP INDEX friend_index ON users ;',
            'DROP INDEX indx ON universal;']
    try:
        for sql in sqls:
            cursor.execute(sql)
    except Exception as code:
        logger.debug('PyMySQL Error: {}'.format(code))
        pass

    cursor.close()
    conn.commit()
    conn.close()


def delete_from_table(table):
    """Delete all rows from a table

    Parameters:
        table - an existing sql table in benchmark_db

    Returns:

    """
    client = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    cursor = client.cursor()
    sql = 'DELETE from {};'.format(table)
    try:
        cursor.execute(sql)
    except Exception as code:
        logger.warning('PyMySQL Error: {}'.format(code))

    client.commit()
    cursor.close()
    client.close()


# TODO: tidy


def bulk_insert_universal(path, indexed, drop_on_start, drop_on_exit=False):
    # if indexed:
    ##    table = 'universal_indexed'
    # else:
    #    table = 'universal'

    table = 'universal'

    if indexed:
        create_indexes()
    else:
        remove_indexes()


#    get_bulk_insert_statement(table,path)
    if drop_on_start: delete_from_table(table=table)

    connector = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)

    cursor = connector.cursor()

    # sql = 'SET NAMES utf8mb4;'
    # cursor.execute(sql)

    execution_time = 0

    try:
        start_time = time.time()
        st = get_bulk_insert_statement(table,path)
        bulk_insert_statement = "{};".format(st[0:-1])
        cursor.execute(bulk_insert_statement)
        execution_time += time.time() - start_time
    except pymysql.Error as code:
        logger.debug('PyMySQL Error: {}'.format(code))
        print(code)


    # for sql in statements:
    #
    #     try:
    #         start_time = time.time()
    #         cursor.execute(sql)
    #         execution_time += time.time() - start_time
    #     except pymysql.Error as code:
    #         logger.debug('PyMySQL Error: {}'.format(code))
    #         pass

    cursor.close()
    connector.commit()
    connector.close()

    size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))
    logger.info("{} seconds to bulk insert {}, indexed={}".format(execution_time, size, indexed))

    if drop_on_exit: delete_from_table(table=table)

    return execution_time, size


def bulk_insert_normalized(path, indexed, drop_on_start, drop_on_exit=False):

    if drop_on_start:
        delete_from_table('hashtags')
        delete_from_table('symbols')
        delete_from_table('media')
        delete_from_table('tweets')
        delete_from_table('users')
        delete_from_table('user_mentions')
        delete_from_table('urls')

    if indexed: create_indexes()
    else: remove_indexes()

    #conn = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    conn = mysql.connector.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)

    cursor = conn.cursor()

    tweet_stmts, user_stmts, hashtags_stmts, media_stmts, user_mention_stmts, url_stmts, symbols_stmts = get_normalized_bulk_insert_statements()

    tweet_bulk_stmt = "{};".format(tweet_stmts[0:-1])
    user_bulk_stmt = "{};".format(user_stmts[0:-1])
    hashtags_bulk_stmt = "{};".format(hashtags_stmts[0:-1])
    media_bulk_stmt = "{};".format(media_stmts[0:-1])
    user_mention_bulk_stmt = "{};".format(user_mention_stmts[0:-1])
    url_bulk_stmt = "{};".format(url_stmts[0:-1])
    symbols_bulk_stmt = "{};".format(symbols_stmts[0:-1])
    run = 0
    start = time.time()

    try:
        cursor.execute(tweet_bulk_stmt)
        cursor.execute(user_bulk_stmt)
        cursor.execute(hashtags_bulk_stmt)
        cursor.execute(media_bulk_stmt)
        cursor.execute(user_mention_bulk_stmt)
        cursor.execute(url_bulk_stmt)
        cursor.execute(symbols_bulk_stmt)
    except Exception as e:
        pass

    run += time.time() - start

    cursor.close()
    try:
        conn.commit()
    except Exception as e:
        pass

    conn.close()

    size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))
    logger.info("{} seconds to bulk insert normalized {}".format(run, size))
    return run, size


def bulk_insert_one_universal(path, indexed):
    if indexed:
       create_indexes()

    else:
       remove_indexes()

    connector = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    # connector = mysql.connector.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)

    cursor = connector.cursor()
    sql = 'SET NAMES utf8mb4;'
    cursor.execute(sql)

    stmts = get_statements('universal')
    run = 0
    for sql in stmts:
        start = time.time()
        try:
            cursor.execute(sql)
            #connector.commit()
        except Exception as e:
            pass
            # print(e)

        run += time.time() - start

    cursor.close()
    connector.commit()
    connector.close()

    size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))
    logger.info("{} seconds to insert one universal {}".format(run, size))
    return run, size


def bulk_insert_one_normalized(path, indexed=False):
    delete_from_table('hashtags')
    delete_from_table('symbols')
    delete_from_table('media')
    delete_from_table('tweets')
    delete_from_table('users')
    delete_from_table('user_mentions')
    delete_from_table('urls')

    # conn = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    conn = mysql.connector.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)

    cursor = conn.cursor()
    # sql = 'SET NAMES utf8mb4;'
    # cursor.execute(sql)

    tweet_stmts, user_stmts, hashtags_stmts, media_stmts, user_mention_stmts, url_stmts, symbols_stmts = get_normalized_statements()

    run = 0

    for sql in tweet_stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(e)
            pass
        run += time.time() - start
    for sql in user_stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(e)
            pass
        run += time.time() - start
    for sql in hashtags_stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(e)

            pass
        run += time.time() - start
    for sql in media_stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(e)

            pass
        run += time.time() - start
    for sql in user_mention_stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(e)

            pass
        run += time.time() - start
    for sql in url_stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(e)

            pass
        run += time.time() - start
    for sql in symbols_stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(e)

            pass
        run += time.time() - start

    cursor.close()
    conn.commit()
    conn.close()

    size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))
    logger.info("{} seconds to bulk_insert_one_normalized {}".format(run, size))
    return run, size


def insert_one_universal(path, indexed, drop_on_start=True, drop_on_exit=False):
    table = 'universal'

    if indexed:
        create_indexes()
    else:
        remove_indexes()

    get_bulk_insert_statement(table, path)
    if drop_on_start: delete_from_table(table=table)

    connector = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)

    cursor = connector.cursor()

    # sql = 'SET NAMES utf8mb4;'
    # cursor.execute(sql)

    execution_time = 0

    try:
        st = get_bulk_insert_statement(table, path)
        bulk_insert_statement = "{};".format(st[0:-1])
        cursor.execute(bulk_insert_statement)
    except pymysql.Error as code:
        logger.debug('PyMySQL Error: {}'.format(code))
        pass

    try:
        start_time = time.time()
        st = get_bulk_insert_statement(table, DOCUMENT_SINGLE)
        bulk_insert_statement = "{};".format(st[0:-1])
        cursor.execute(bulk_insert_statement)
        execution_time += time.time() - start_time
    except pymysql.Error as code:
        logger.debug('PyMySQL Error: {}'.format(code))
        pass


    doc_size = "{}MB".format(round(os.path.getsize(DOCUMENT_SINGLE) / 1024 / 1024, 2))
    db_size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))

    logger.info("{} seconds to insert_one_universal indexed={} db_size={} doc_size={}".format(execution_time, indexed, db_size,
                                                                                    doc_size))
    return execution_time, db_size, doc_size


def insert_one_normalized(path, indexed, drop_on_start=True, drop_on_exit=False):
    delete_from_table('hashtags')
    delete_from_table('symbols')
    delete_from_table('media')
    delete_from_table('tweets')
    delete_from_table('users')
    delete_from_table('user_mentions')
    delete_from_table('urls')

    #conn = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    conn = mysql.connector.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)

    cursor = conn.cursor()

    tweet_stmts, user_stmts, hashtags_stmts, media_stmts, user_mention_stmts, url_stmts, symbols_stmts = get_normalized_bulk_insert_statements()

    tweet_bulk_stmt = "{};".format(tweet_stmts[0:-1])
    user_bulk_stmt = "{};".format(user_stmts[0:-1])
    hashtags_bulk_stmt = "{};".format(hashtags_stmts[0:-1])
    media_bulk_stmt = "{};".format(media_stmts[0:-1])
    user_mention_bulk_stmt = "{};".format(user_mention_stmts[0:-1])
    url_bulk_stmt = "{};".format(url_stmts[0:-1])
    symbols_bulk_stmt = "{};".format(symbols_stmts[0:-1])

    try:
        cursor.execute(tweet_bulk_stmt)
        cursor.execute(user_bulk_stmt)
        cursor.execute(hashtags_bulk_stmt)
        cursor.execute(media_bulk_stmt)
        cursor.execute(user_mention_bulk_stmt)
        cursor.execute(url_bulk_stmt)
        cursor.execute(symbols_bulk_stmt)

    except Exception as e:
        pass

    execution_time = 0

    try:
        start_time = time.time()

        sql_tweets, sql_users, sql_hashtags, sql_media, sql_user_mentions, sql_urls, sql_symbols = get_normalized_statements()

        try:
            cursor.execute(sql_tweets)
            cursor.execute(sql_users)
            cursor.execute(sql_hashtags)
            cursor.execute(sql_media)
            cursor.execute(sql_user_mentions)
            cursor.execute(sql_urls)
            cursor.execute(sql_symbols)

        except Exception as e:
            pass

        #bulk_insert_statement = "{};".format(st[0:-1])

        #cursor.execute(bulk_insert_statement)

        execution_time += time.time() - start_time


    except pymysql.Error as code:
        logger.debug('PyMySQL Error: {}'.format(code))
        pass


    doc_size = "{}MB".format(round(os.path.getsize(DOCUMENT_SINGLE) / 1024 / 1024, 2))
    db_size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))

    logger.info("{} seconds to insert_one_normzlized indexed={} db_size={} doc_size={}".format(execution_time, indexed, db_size,
                                                                                    doc_size))

    return execution_time, db_size, doc_size


def select_universal(path, indexed):
    conn = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    cursor = conn.cursor()

    if indexed:
        create_indexes()
    else:
        remove_indexes()

    # if indexed:
    #    sql1 = "SELECT COUNT(*) FROM universal_indexed where `users.location` = 'London';"
    #    sql2 = "SELECT COUNT(*) FROM universal_indexed WHERE `users.friends_count`>1000;"
    #    sql3 = "SELECT COUNT(*) FROM universal_indexed WHERE `users.followers_count`>1000;"
    # else:
    #    sql1 = "SELECT COUNT(*) FROM universal WHERE `users.location` = 'London';"
    #    sql2 = "SELECT COUNT(*) FROM universal WHERE `users.friends_count`>1000;"
    #    sql3 = "SELECT COUNT(*) FROM universal WHERE `users.followers_count`>1000;"

    sqls = ["SELECT COUNT(*) FROM universal WHERE `users.location` = 'London';",
            "SELECT COUNT(*) FROM universal WHERE `users.friends_count`>1000;",
            "SELECT COUNT(*) FROM universal WHERE `users.followers_count`>1000;"]

    count = 0
    sql_time = 0

    for sql in sqls:
        start = time.time()
        cursor.execute(sql)
        sql_time += time.time() - start
        res = cursor.fetchone()
        for row in res: count += row

    size = "{}MB".format(round(os.path.getsize(path) / 1024 / 1024, 2))
    logger.info("{} seconds to select_universal {} objects indexed={}, doc_size={}".format(sql_time, count, indexed, size))

    return sql_time, count


def select_normalized(path, indexed):
    conn = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    cursor = conn.cursor()

    if indexed:
        create_indexes()
    else:
        remove_indexes()

    sqls = ["SELECT COUNT(*) FROM users WHERE `location` = 'London';",
            "SELECT COUNT(*) FROM users WHERE `friends_count`>1000;",
            "SELECT COUNT(*) FROM users WHERE `followers_count`>1000;"]

    num = 0
    sql_time = 0

    for sql in sqls:
        start = time.time()
        cursor.execute(sql)
        sql_time += time.time() - start
        res = cursor.fetchone()
        for row in res: num += row

    size = "{}MB".format(round(os.path.getsize(path) / 1024 / 1024, 2))
    logger.info("{} seconds to select_normalized {} objects indexed={}, doc_size={}".format(sql_time, num, indexed, size))

    return sql_time, num


def scan_universal():
    """Scan from a table (default = universal)

    Parameters:
        table - an existing sql table in benchmark_db

    Returns:

    """
    conn = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    cursor = conn.cursor()

    sql = "SELECT * FROM universal;"

    start_time = time.time()

    cursor.execute(sql)

    execution_time = time.time() - start_time

    cursor.fetchall()

    scanned = cursor.rowcount

    logger.info(" %s seconds to scan %d objects from universal table", execution_time, scanned)

    return execution_time, scanned


def scan_normalized():
    """Scan from a table

    Parameters:
        table - an existing sql table in benchmark_db

    Returns:

    """
    conn = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    cursor = conn.cursor()

    scanned = 0

    start_time = time.time()

    sql = "SELECT * FROM users;"
    cursor.execute(sql)
    cursor.fetchall()
    scanned += cursor.rowcount
    sql = "SELECT * FROM tweets;"
    cursor.execute(sql)
    cursor.fetchall()
    scanned += cursor.rowcount
    sql = "SELECT * FROM hashtags;"
    cursor.execute(sql)
    cursor.fetchall()
    scanned += cursor.rowcount
    sql = "SELECT * FROM urls;"
    cursor.execute(sql)
    cursor.fetchall()
    scanned += cursor.rowcount
    sql = "SELECT * FROM symbols;"
    cursor.execute(sql)
    cursor.fetchall()
    scanned += cursor.rowcount
    sql = "SELECT * FROM media;"
    cursor.execute(sql)
    cursor.fetchall()
    scanned += cursor.rowcount
    sql = "SELECT * FROM user_mentions;"
    cursor.execute(sql)
    cursor.fetchall()
    scanned += cursor.rowcount

    execution_time = time.time() - start_time

    # cursor.fetchall()
    # scanned = cursor.rowcount

    logger.info(" {} seconds to scan_normalized {} objects".format(execution_time, scanned))

    return execution_time, scanned


#############################
#  Generate SQL Statements  #
#############################


def get_statements(table, path=DOCUMENT):
    document = io.open(path, 'r')

    values = []

    stmts = []

    with document as json_docs:
        for data in json_docs:
            data = json.loads(data)

            # tweet

            tweet_created_at = str(data['created_at'])
            tweet_id = str(data['id'])
            tweet_id_str = str(data['id_str'])
            tweet_text = str(data['text']).replace("'", "\\'")
            tweet_source = str(data['source']).replace("\'", "\\'")
            tweet_truncated = str(data['truncated'])
            tweet_in_reply_to_status_id = str(data['in_reply_to_status_id'])
            tweet_in_reply_to_status_id_str = str(data['in_reply_to_status_id_str'])
            tweet_in_reply_to_user_id = str(data['in_reply_to_user_id'])
            tweet_in_reply_to_user_id_str = str(data['in_reply_to_user_id_str'])
            tweet_in_reply_to_screen_name = str(data['in_reply_to_screen_name'])
            tweet_quote_count = str(data['quote_count'])
            tweet_reply_count = str(data['reply_count'])
            tweet_favorite_count = str(data['favorite_count'])
            tweet_favorited = str(data['favorited'])
            tweet_retweeted = str(data['retweeted'])
            tweet_filter_level = str(data['filter_level'])
            tweet_lang = str(data['lang'])
            tweet_coordinates = data['coordinates']['coordinates'] if 'type' in data else None
            tweet_coordinates_type = data['coordinates']['type'] if 'type' in data else None
            place_country = str(data['place']['country']) if 'country' in data else None
            place_country_code = str(data['place']['country_code']) if 'country' in data else None
            place_full_name = str(data['place']['full_name']) if 'country' in data else None
            place_id = str(data['place']['id']) if 'country' in data else None
            place_name = str(data['place']['name']) if 'country' in data else None
            place_place_type = str(data['place']['place_type']) if 'country' in data else None
            place_url = str(data['place']['url']) if 'country' in data else None
            quoted_status_id = str(data['quoted_status_id']) if 'quoted_status_id' in data else None
            quoted_status_id_str = str(data['quoted_status_id_str']) if 'quoted_status_id' in data else None
            quoted_status = str(data['quoted_status']['text']).replace("\'", "\\'") if 'quoted_status' in data else None
            possibly_sensitive = str(data['possibly_sensitive']) if 'possibly_sensitive' in data else None
            retweeted_status = str(data['retweeted_status']['id']) if 'retweeted_status' in data else None

            # user

            user_id = str(data['user']['id'])
            user_id_str = str(data['user']['id_str'])
            name = str(data['user']['name']).replace("\'", "\\'")
            screen_name = str(data['user']['screen_name']).replace("\'", "\\'")
            location = str(data['user']['location']).replace("\'", "\\'")
            user_url = str(data['user']['url']).replace("\'", "\\'")
            description = str(data['user']['description']).replace("\'", "\\""")
            description = description.replace("\\", "|")
            translator_type = str(data['user']['translator_type']).replace("\'", "\\'")
            protected = str(data['user']['protected'])
            verified = str(data['user']['verified'])
            followers_count = str(data['user']['followers_count'])
            friends_count = str(data['user']['friends_count'])
            listed_count = str(data['user']['listed_count'])
            favourites_count = str(data['user']['favourites_count'])
            statuses_count = str(data['user']['statuses_count'])
            user_created_at = str(data['user']['created_at'])
            utc_offset = str(data['user']['utc_offset'])
            time_zone = str(data['user']['time_zone']).replace("\'", "\\'")
            geo_enabled = str(data['user']['geo_enabled'])
            user_lang = str(data['user']['lang'])
            contributors_enabled = str(data['user']['contributors_enabled'])
            is_translator = str(data['user']['is_translator'])
            profile_background_color = str(data['user']['profile_background_color'])
            profile_background_image_url = str(data['user']['profile_background_image_url'])
            profile_background_image_url_https = str(data['user']['profile_background_image_url_https'])
            profile_background_tile = str(data['user']['profile_background_tile'])
            profile_image_url = str(data['user']['profile_image_url'])
            profile_image_url_https = str(data['user']['profile_image_url_https'])
            profile_link_color = str(data['user']['profile_link_color'])
            profile_sidebar_border_color = str(data['user']['profile_sidebar_border_color'])
            profile_sidebar_fill_color = str(data['user']['profile_sidebar_fill_color'])
            profile_text_color = str(data['user']['profile_text_color'])
            profile_use_background_image = str(data['user']['profile_use_background_image'])
            default_profile = str(data['user']['default_profile'])
            default_profile_image = str(data['user']['default_profile_image'])
            following = str(data['user']['default_profile_image'])
            follow_request_sent = str(data['user']['follow_request_sent'])
            notifications = str(data['user']['notifications'])

            # hashtag

            hashtag_text = data['entities']['hashtags']

            hashtag_id = 0
            hashtag_htext = None
            hashtag_indices = None

            for hashtag in hashtag_text:
                hashtag_id = data['id']
                hashtag_htext = str(hashtag['text'])
                hashtag_indices = str(hashtag['indices'])

            # symbols

            symbol_text = data['entities']['symbols']

            symbol_id = None
            symbol_symbol_text = None
            symbol_indices = None

            for symbol in symbol_text:
                symbol_id = 0
                symbol_symbol_text = str(symbol['text'])
                symbol_indices = str(symbol['indices'])

            # urls

            url_text = data['entities']['urls']

            url_id = 0
            url_url = None
            url_display_url_ = None
            url_expanded_url = None
            url_indices = None

            for url in url_text:
                url_id = data['id']
                url_url = str(url['url'])
                url_display_url_ = str(url['display_url'])
                url_expanded_url = str(url['expanded_url'])
                url_indices = str(url['indices'])

            # user mentions

            user_mention_text = data['entities']['user_mentions']

            user_mention_name = None
            user_mention_indices = None
            user_mention_screen_name = None
            user_mention_id = 0
            user_mention_id_str = None

            for user_mention in user_mention_text:
                user_mention_name = str(user_mention['name']).replace("\'", "\\'")
                user_mention_indices = str(user_mention['indices'])
                user_mention_screen_name = str(user_mention['screen_name']).replace("\'", "\\'")
                user_mention_id = str(user_mention['id'])
                user_mention_id_str = str(user_mention['id_str'])

            # media

            media_tweet_id = 0
            media_type = None
            media_media = None
            media_sizes = None
            media_indices = None
            media_media_url = None
            media_display_url = None
            media_id = 0
            media_id_str = None
            media_expanded_url = None
            media_media_url_https = None

            if 'media' in data['entities']:

                media_text = data['entities']['media']

                for media in media_text:
                    media_tweet_id = data['id']
                    media_type = str(media['type'])
                    media_sizes = None
                    media_indices = str(media['indices'])
                    media_media = str(media['url'])
                    media_media_url = str(media['media_url'])
                    media_display_url = str(media['display_url'])
                    media_id = str(media['id'])
                    media_id_str = str(media['id_str'])
                    media_expanded_url = str(media['expanded_url'])
                    media_media_url_https = str(media['media_url_https'])

            stmts.append("INSERT INTO {} VALUES (0," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');".format(
                table,
                hashtag_htext, hashtag_id, hashtag_indices, media_display_url, media_expanded_url, media_tweet_id,
                media_indices, media_id, media_id_str, media_media, media_media_url, media_media_url_https,
                media_sizes, media_type, symbol_id, symbol_indices, symbol_symbol_text, tweet_coordinates,
                tweet_coordinates_type, tweet_created_at, tweet_favorite_count, tweet_favorited, tweet_filter_level,
                tweet_id, tweet_id_str, tweet_in_reply_to_screen_name, tweet_in_reply_to_status_id,
                tweet_in_reply_to_status_id_str, tweet_in_reply_to_user_id, tweet_in_reply_to_user_id_str, tweet_lang,
                place_country, place_country_code, place_full_name, place_id, place_name, place_place_type, place_url,
                possibly_sensitive, tweet_quote_count, quoted_status, quoted_status_id, quoted_status_id_str,
                tweet_reply_count, tweet_retweeted, retweeted_status, tweet_source, tweet_text, tweet_truncated,
                user_id, url_display_url_, url_expanded_url, url_id, url_indices, url_url, user_mention_id,
                user_mention_indices, user_mention_name, user_mention_screen_name, user_mention_id, user_mention_id_str,
                contributors_enabled, user_created_at, default_profile, default_profile_image, description,
                favourites_count, follow_request_sent, followers_count, following, friends_count, geo_enabled,
                user_id, user_id_str, is_translator, user_lang, listed_count, location, name, notifications,
                profile_background_color, profile_background_image_url, profile_background_image_url_https,
                profile_background_tile, profile_image_url, profile_image_url_https, profile_link_color,
                profile_sidebar_border_color, profile_sidebar_fill_color, profile_text_color,
                profile_use_background_image, protected, screen_name, statuses_count, time_zone, translator_type,
                user_url, utc_offset, verified))

            values.append("(0," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');".format(

                hashtag_htext, hashtag_id, hashtag_indices, media_display_url, media_expanded_url, media_tweet_id,
                media_indices, media_id, media_id_str, media_media, media_media_url, media_media_url_https,
                media_sizes, media_type, symbol_id, symbol_indices, symbol_symbol_text, tweet_coordinates,
                tweet_coordinates_type, tweet_created_at, tweet_favorite_count, tweet_favorited, tweet_filter_level,
                tweet_id, tweet_id_str, tweet_in_reply_to_screen_name, tweet_in_reply_to_status_id,
                tweet_in_reply_to_status_id_str, tweet_in_reply_to_user_id, tweet_in_reply_to_user_id_str, tweet_lang,
                place_country, place_country_code, place_full_name, place_id, place_name, place_place_type, place_url,
                possibly_sensitive, tweet_quote_count, quoted_status, quoted_status_id, quoted_status_id_str,
                tweet_reply_count, tweet_retweeted, retweeted_status, tweet_source, tweet_text, tweet_truncated,
                user_id, url_display_url_, url_expanded_url, url_id, url_indices, url_url, user_mention_id,
                user_mention_indices, user_mention_name, user_mention_screen_name, user_mention_id, user_mention_id_str,
                contributors_enabled, user_created_at, default_profile, default_profile_image, description,
                favourites_count, follow_request_sent, followers_count, following, friends_count, geo_enabled,
                user_id, user_id_str, is_translator, user_lang, listed_count, location, name, notifications,
                profile_background_color, profile_background_image_url, profile_background_image_url_https,
                profile_background_tile, profile_image_url, profile_image_url_https, profile_link_color,
                profile_sidebar_border_color, profile_sidebar_fill_color, profile_text_color,
                profile_use_background_image, protected, screen_name, statuses_count, time_zone, translator_type,
                user_url, utc_offset, verified))


    return stmts


def get_bulk_insert_statement(table, path=DOCUMENT):
    document = io.open(path, 'r')

    values = ""

    stmts = []

    with document as json_docs:
        for data in json_docs:
            data = json.loads(data)

            # tweet

            tweet_created_at = str(data['created_at'])
            tweet_id = str(data['id'])
            tweet_id_str = str(data['id_str'])
            tweet_text = str(data['text']).replace("'", "\\'")
            tweet_source = str(data['source']).replace("\'", "\\'")
            tweet_truncated = str(data['truncated']).replace("\'", "\\'")
            tweet_in_reply_to_status_id = str(data['in_reply_to_status_id']).replace("\'", "\\'")
            tweet_in_reply_to_status_id_str = str(data['in_reply_to_status_id_str']).replace("\'", "\\'")
            tweet_in_reply_to_user_id = str(data['in_reply_to_user_id']).replace("\'", "\\'")
            tweet_in_reply_to_user_id_str = str(data['in_reply_to_user_id_str']).replace("\'", "\\'")
            tweet_in_reply_to_screen_name = str(data['in_reply_to_screen_name']).replace("\'", "\\'")
            tweet_quote_count = str(data['quote_count']).replace("\'", "\\'")
            tweet_reply_count = str(data['reply_count']).replace("\'", "\\'")
            tweet_favorite_count = str(data['favorite_count']).replace("\'", "\\'")
            tweet_favorited = str(data['favorited']).replace("\'", "\\'")
            tweet_retweeted = str(data['retweeted']).replace("\'", "\\'")
            tweet_filter_level = str(data['filter_level']).replace("\'", "\\'")
            tweet_lang = str(data['lang']).replace("\'", "\\'")
            tweet_coordinates = data['coordinates']['coordinates'] if 'type' in data else None
            tweet_coordinates_type = data['coordinates']['type'] if 'type' in data else None
            place_country = str(data['place']['country']) if 'country' in data else None
            place_country_code = str(data['place']['country_code']) if 'country' in data else None
            place_full_name = str(data['place']['full_name']) if 'country' in data else None
            place_id = str(data['place']['id']) if 'country' in data else None
            place_name = str(data['place']['name']) if 'country' in data else None
            place_place_type = str(data['place']['place_type']) if 'country' in data else None
            place_url = str(data['place']['url']) if 'country' in data else None
            quoted_status_id = str(data['quoted_status_id']) if 'quoted_status_id' in data else None
            quoted_status_id_str = str(data['quoted_status_id_str']) if 'quoted_status_id' in data else None
            quoted_status = str(data['quoted_status']['text']).replace("\'", "\\'") if 'quoted_status' in data else None
            possibly_sensitive = str(data['possibly_sensitive']) if 'possibly_sensitive' in data else None
            retweeted_status = str(data['retweeted_status']['id']) if 'retweeted_status' in data else None

            # user

            user_id = str(data['user']['id'])
            user_id_str = str(data['user']['id_str'])
            name = str(data['user']['name']).replace("\'", "\\'")
            screen_name = str(data['user']['screen_name']).replace("\'", "\\'")
            location = str(data['user']['location']).replace("\'", "\\'")
            user_url = str(data['user']['url']).replace("\'", "\\'")
            description = str(data['user']['description']).replace("\'", "\\""")
            description = description.replace("\\", "|")
            translator_type = str(data['user']['translator_type']).replace("\'", "\\'")
            protected = str(data['user']['protected']).replace("\'", "\\'")
            verified = str(data['user']['verified']).replace("\'", "\\'")
            followers_count = str(data['user']['followers_count']).replace("\'", "\\'")
            friends_count = str(data['user']['friends_count']).replace("\'", "\\'")
            listed_count = str(data['user']['listed_count']).replace("\'", "\\'")
            favourites_count = str(data['user']['favourites_count']).replace("\'", "\\'")
            statuses_count = str(data['user']['statuses_count']).replace("\'", "\\'")
            user_created_at = str(data['user']['created_at']).replace("\'", "\\'")
            utc_offset = str(data['user']['utc_offset']).replace("\'", "\\'")
            time_zone = str(data['user']['time_zone']).replace("\'", "\\'")
            geo_enabled = str(data['user']['geo_enabled']).replace("\'", "\\'")
            user_lang = str(data['user']['lang']).replace("\'", "\\'")
            contributors_enabled = str(data['user']['contributors_enabled']).replace("\'", "\\'")
            is_translator = str(data['user']['is_translator']).replace("\'", "\\'")
            profile_background_color = str(data['user']['profile_background_color']).replace("\'", "\\'")
            profile_background_image_url = str(data['user']['profile_background_image_url']).replace("\'", "\\'")
            profile_background_image_url_https = str(data['user']['profile_background_image_url_https']).replace("\'", "\\'")
            profile_background_tile = str(data['user']['profile_background_tile']).replace("\'", "\\'")
            profile_image_url = str(data['user']['profile_image_url']).replace("\'", "\\'")
            profile_image_url_https = str(data['user']['profile_image_url_https']).replace("\'", "\\'")
            profile_link_color = str(data['user']['profile_link_color']).replace("\'", "\\'")
            profile_sidebar_border_color = str(data['user']['profile_sidebar_border_color']).replace("\'", "\\'")
            profile_sidebar_fill_color = str(data['user']['profile_sidebar_fill_color']).replace("\'", "\\'")
            profile_text_color = str(data['user']['profile_text_color']).replace("\'", "\\'")
            profile_use_background_image = str(data['user']['profile_use_background_image']).replace("\'", "\\'")
            default_profile = str(data['user']['default_profile']).replace("\'", "\\'")
            default_profile_image = str(data['user']['default_profile_image']).replace("\'", "\\'")
            following = str(data['user']['default_profile_image']).replace("\'", "\\'")
            follow_request_sent = str(data['user']['follow_request_sent']).replace("\'", "\\'")
            notifications = str(data['user']['notifications']).replace("\'", "\\'")

            # hashtag

            hashtag_text = data['entities']['hashtags']

            hashtag_id = 0
            hashtag_htext = None
            hashtag_indices = None

            for hashtag in hashtag_text:
                hashtag_id = data['id']
                hashtag_htext = str(hashtag['text']).replace("\'", "\\'")
                hashtag_indices = str(hashtag['indices']).replace("\'", "\\'")

            # symbols

            symbol_text = data['entities']['symbols']

            symbol_id = None
            symbol_symbol_text = None
            symbol_indices = None

            for symbol in symbol_text:
                symbol_id = 0
                symbol_symbol_text = str(symbol['text']).replace("\'", "\\'")
                symbol_indices = str(symbol['indices']).replace("\'", "\\'")

            # urls

            url_text = data['entities']['urls']

            url_id = 0
            url_url = None
            url_display_url_ = None
            url_expanded_url = None
            url_indices = None

            for url in url_text:
                url_id = data['id']
                url_url = str(url['url'])
                url_display_url_ = str(url['display_url']).replace("\'", "\\'")
                url_expanded_url = str(url['expanded_url']).replace("\'", "\\'")
                url_indices = str(url['indices']).replace("\'", "\\'")

            # user mentions

            user_mention_text = data['entities']['user_mentions']

            user_mention_name = None
            user_mention_indices = None
            user_mention_screen_name = None
            user_mention_id = 0
            user_mention_id_str = None

            for user_mention in user_mention_text:
                user_mention_name = str(user_mention['name']).replace("\'", "\\'")
                user_mention_indices = str(user_mention['indices']).replace("\'", "\\'")
                user_mention_screen_name = str(user_mention['screen_name']).replace("\'", "\\'")
                user_mention_id = str(user_mention['id']).replace("\'", "\\'")
                user_mention_id_str = str(user_mention['id_str']).replace("\'", "\\'")

            # media

            media_tweet_id = 0
            media_type = None
            media_media = None
            media_sizes = None
            media_indices = None
            media_media_url = None
            media_display_url = None
            media_id = 0
            media_id_str = None
            media_expanded_url = None
            media_media_url_https = None

            if 'media' in data['entities']:

                media_text = data['entities']['media']

                for media in media_text:
                    media_tweet_id = data['id']
                    media_type = str(media['type']).replace("\'", "\\'")
                    media_sizes = None
                    media_indices = str(media['indices']).replace("\'", "\\'")
                    media_media = str(media['url']).replace("\'", "\\'")
                    media_media_url = str(media['media_url']).replace("\'", "\\'")
                    media_display_url = str(media['display_url']).replace("\'", "\\'")
                    media_id = str(media['id']).replace("\'", "\\'")
                    media_id_str = str(media['id_str']).replace("\'", "\\'")
                    media_expanded_url = str(media['expanded_url']).replace("\'", "\\'")
                    media_media_url_https = str(media['media_url_https']).replace("\'", "\\'")


            values +=("(0," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'," \
                         "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'),".format(

                hashtag_htext, hashtag_id, hashtag_indices, media_display_url, media_expanded_url, media_tweet_id,
                media_indices, media_id, media_id_str, media_media, media_media_url, media_media_url_https,
                media_sizes, media_type, symbol_id, symbol_indices, symbol_symbol_text, tweet_coordinates,
                tweet_coordinates_type, tweet_created_at, tweet_favorite_count, tweet_favorited, tweet_filter_level,
                tweet_id, tweet_id_str, tweet_in_reply_to_screen_name, tweet_in_reply_to_status_id,
                tweet_in_reply_to_status_id_str, tweet_in_reply_to_user_id, tweet_in_reply_to_user_id_str, tweet_lang,
                place_country, place_country_code, place_full_name, place_id, place_name, place_place_type, place_url,
                possibly_sensitive, tweet_quote_count, quoted_status, quoted_status_id, quoted_status_id_str,
                tweet_reply_count, tweet_retweeted, retweeted_status, tweet_source, tweet_text, tweet_truncated,
                user_id, url_display_url_, url_expanded_url, url_id, url_indices, url_url, user_mention_id,
                user_mention_indices, user_mention_name, user_mention_screen_name, user_mention_id, user_mention_id_str,
                contributors_enabled, user_created_at, default_profile, default_profile_image, description,
                favourites_count, follow_request_sent, followers_count, following, friends_count, geo_enabled,
                user_id, user_id_str, is_translator, user_lang, listed_count, location, name, notifications,
                profile_background_color, profile_background_image_url, profile_background_image_url_https,
                profile_background_tile, profile_image_url, profile_image_url_https, profile_link_color,
                profile_sidebar_border_color, profile_sidebar_fill_color, profile_text_color,
                profile_use_background_image, protected, screen_name, statuses_count, time_zone, translator_type,
                user_url, utc_offset, verified))


    sql = "INSERT INTO universal VALUES {}".format(values)

    return sql


def get_normalized_statements(path=DOCUMENT):
    tweet_stmts = []
    user_stmts = []
    media_stmts = []
    user_mention_stmts = []
    url_stmts = []
    symbols_stmts = []
    hashtags_stmts = []

    id = 0

    document = io.open(path, 'r')

    with document as json_docs:
        for data in json_docs:

            data = json.loads(data)

            # tweet

            created_at = str(data['created_at'])
            t_id = str(data['id'])
            t_id_str = str(data['id_str'])
            text = str(data['text']).replace("'", "\\'")
            source = str(data['source']).replace("\'", "\\'")
            truncated = str(data['truncated']).replace("\'", "\\'")
            in_reply_to_status_id = str(data['in_reply_to_status_id']).replace("\'", "\\'")
            in_reply_to_status_id_str = str(data['in_reply_to_status_id_str']).replace("\'", "\\'")
            in_reply_to_user_id = str(data['in_reply_to_user_id']).replace("\'", "\\'")
            in_reply_to_user_id_str = str(data['in_reply_to_user_id_str']).replace("\'", "\\'")
            in_reply_to_screen_name = str(data['in_reply_to_screen_name']).replace("\'", "\\'")

            if in_reply_to_user_id == 'None': in_reply_to_user_id = 0
            if in_reply_to_status_id == 'None': in_reply_to_status_id = 0

            user_id = str(data['user']['id'])
            quote_count = str(data['quote_count'])
            reply_count = str(data['reply_count'])
            favorite_count = str(data['favorite_count']).replace("\'", "\\'")
            favorited = str(data['favorited']).replace("\'", "\\'")
            retweeted = str(data['retweeted']).replace("\'", "\\'")
            filter_level = str(data['filter_level']).replace("\'", "\\'")
            lang = str(data['lang']).replace("\'", "\\'")
            tweet_coordinates = data['coordinates']['coordinates'] if 'type' in data else None
            tweet_coordinates_type = data['coordinates']['type'] if 'type' in data else None
            place_country = str(data['place']['country']) if 'country' in data else None
            place_country_code = str(data['place']['country_code']) if 'country' in data else None
            place_full_name = str(data['place']['full_name']) if 'country' in data else None
            place_id = str(data['place']['id']) if 'country' in data else 0
            place_name = str(data['place']['name']) if 'country' in data else None
            place_place_type = str(data['place']['place_type']) if 'country' in data else None
            place_url = str(data['place']['url']) if 'country' in data else None
            quoted_status_id = str(data['quoted_status_id']) if 'quoted_status_id' in data else 0
            quoted_status_id_str = str(data['quoted_status_id_str']) if 'quoted_status_id' in data else None
            quoted_status = str(data['quoted_status']['text']).replace("\'", "\\'") if 'quoted_status' in data else None
            possibly_sensitive = str(data['possibly_sensitive']) if 'possibly_sensitive' in data else None
            retweeted_status = str(data['retweeted_status']['id']) if 'retweeted_status' in data else None


            try: media_id = str(data['entities']['media'][0]['id'])
            except: media_id= None

            try: user_mentions_id = str(data['entities']['user_mentions'][0]['id'])
            except: user_mentions_id=None

            try: urls_id= str(data['entities']['urls'][0]['id'])
            except: urls_id = None

            try: hashtags_id = str(data['entities']['hashtags'][0]['id'])
            except: hashtags_id=None

            try: symbols_id = str(data['entities']['symbols'][0]['id'])
            except: symbols_id = None

            # note the IGNORE here - there might be duplicate tweets in the data source

            tweet_stmts.append("INSERT IGNORE INTO tweets VALUES (" \
                               "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', " \
                               "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', " \
                               "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');".format(
                created_at, t_id, t_id_str, text, source, truncated, in_reply_to_status_id, in_reply_to_status_id_str,
                in_reply_to_user_id, in_reply_to_user_id_str, in_reply_to_screen_name, user_id, tweet_coordinates,
                tweet_coordinates_type, place_country, place_country_code, place_full_name, place_id, place_name,
                place_place_type, place_url, quote_count, reply_count, favorite_count, favorited, retweeted,
                filter_level, lang, quoted_status_id, quoted_status_id_str, quoted_status, possibly_sensitive,
                retweeted_status, media_id, user_mentions_id, urls_id, hashtags_id, symbols_id
            ))

            # user table

            u_id = str(data['user']['id'])
            u_id_str = str(data['user']['id_str'])
            name = str(data['user']['name']).replace("\'", "\\'")
            screen_name = str(data['user']['screen_name']).replace("\'", "\\'")
            location = str(data['user']['location']).replace("\'", "\\'")
            url = str(data['user']['url']).replace("\'", "\\'")
            description = str(data['user']['description']).replace("\'", "\\""")
            description = description.replace("\\", "|")
            translator_type = str(data['user']['translator_type']).replace("\'", "\\'")
            protected = str(data['user']['protected']).replace("\'", "\\'")
            verified = str(data['user']['verified']).replace("\'", "\\'")
            followers_count = str(data['user']['followers_count']).replace("\'", "\\'")
            friends_count = str(data['user']['friends_count']).replace("\'", "\\'")
            listed_count = str(data['user']['listed_count']).replace("\'", "\\'")
            favourites_count = str(data['user']['favourites_count']).replace("\'", "\\'")
            statuses_count = str(data['user']['statuses_count']).replace("\'", "\\'")
            created_at = str(data['user']['created_at']).replace("\'", "\\'")
            utc_offset = str(data['user']['utc_offset']).replace("\'", "\\'")
            time_zone = str(data['user']['time_zone']).replace("\'", "\\'")
            geo_enabled = str(data['user']['geo_enabled']).replace("\'", "\\'")
            lang = str(data['user']['lang']).replace("\'", "\\'")
            contributors_enabled = str(data['user']['contributors_enabled']).replace("\'", "\\'")
            is_translator = str(data['user']['is_translator']).replace("\'", "\\'")
            profile_background_color = str(data['user']['profile_background_color']).replace("\'", "\\'")
            profile_background_image_url = str(data['user']['profile_background_image_url']).replace("\'", "\\'")
            profile_background_image_url_https = str(data['user']['profile_background_image_url_https']).replace("\'", "\\'")
            profile_background_tile = str(data['user']['profile_background_tile']).replace("\'", "\\'")
            profile_image_url = str(data['user']['profile_image_url']).replace("\'", "\\'")
            profile_image_url_https = str(data['user']['profile_image_url_https']).replace("\'", "\\'")
            profile_link_color = str(data['user']['profile_link_color']).replace("\'", "\\'")
            profile_sidebar_border_color = str(data['user']['profile_sidebar_border_color']).replace("\'", "\\'")
            profile_sidebar_fill_color = str(data['user']['profile_sidebar_fill_color']).replace("\'", "\\'")
            profile_text_color = str(data['user']['profile_text_color']).replace("\'", "\\'")
            profile_use_background_image = str(data['user']['profile_use_background_image']).replace("\'", "\\'")
            default_profile = str(data['user']['default_profile']).replace("\'", "\\'")
            default_profile_image = str(data['user']['default_profile_image']).replace("\'", "\\'")
            following = str(data['user']['default_profile_image']).replace("\'", "\\'")
            follow_request_sent = str(data['user']['follow_request_sent']).replace("\'", "\\'")
            notifications = str(data['user']['notifications']).replace("\'", "\\'")

            user_stmts.append("INSERT IGNORE INTO users VALUES (" \
                              "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', " \
                              "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'" \
                              "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');".format(
                u_id, u_id_str, name, screen_name, location, url, description, translator_type, protected,
                verified, followers_count, friends_count, listed_count, favourites_count, statuses_count,
                created_at, utc_offset, time_zone, geo_enabled, lang, contributors_enabled, is_translator,
                profile_background_color, profile_background_image_url, profile_background_image_url_https,
                profile_background_tile, profile_image_url, profile_image_url_https, profile_link_color,
                profile_sidebar_border_color, profile_sidebar_fill_color, profile_text_color,
                profile_use_background_image, default_profile, default_profile_image, following, follow_request_sent,
                notifications))

            # hashtag

            hashtag_text = data['entities']['hashtags']

            for hashtag in hashtag_text:
                id = data['id']
                hashtag_text = str(hashtag['text']).replace("\'", "\\'")
                indices_text = str(hashtag['indices']).replace("\'", "\\'")

                hashtags_stmts.append(
                    "INSERT IGNORE into hashtags (id, hashtag, indices) VALUES ({}, '{}', '{}');".format(
                        id, hashtag_text, indices_text))

            # urls

            url_text = data['entities']['urls']

            for url in url_text:
                id = data['id']
                url_text = str(url['url']).replace("\'", "\\'")
                display_url_text = str(url['display_url']).replace("\'", "\\'")
                expanded_url_text = str(url['expanded_url']).replace("\'", "\\'")
                indices_text = str(url['indices']).replace("\'", "\\'")

                url_stmts.append("INSERT IGNORE into urls VALUES ({}, '{}', '{}', '{}', '{}');".format(
                    id, url_text, display_url_text, expanded_url_text, indices_text))

            # symbols

            symbol_text = data['entities']['symbols']

            for symbol in symbol_text:
                id = 0
                symbol_text = str(symbol['text']).replace("\'", "\\'")
                indices_text = str(symbol['indices']).replace("\'", "\\'")

                symbols_stmts.append(
                    "INSERT IGNORE into symbols VALUES ({}, '{}', '{}');".format(id, symbol_text, indices_text))

            # user mentions
            user_mention_text = data['entities']['user_mentions']

            for user_mention in user_mention_text:
                id = data['id']
                name = str(user_mention['name']).replace("\'", "\\'")

                # print(name.replace("\\","|"))

                indices = str(user_mention['indices'])
                screen_name = str(user_mention['screen_name']).replace("\'", "\\'")
                u_id = str(user_mention['id']).replace("\'", "\\'")
                u_id_str = str(user_mention['id_str']).replace("\'", "\\'")

                user_mention_stmts.append(
                    "INSERT IGNORE into user_mentions VALUES ({}, '{}','{}','{}','{}','{}');".format(id, name, indices,
                                                                                                     screen_name, u_id,
                                                                                                     u_id_str))

            # media

            if 'media' in data['entities']:

                media_text = data['entities']['media']

                for media in media_text:
                    id = data['id']
                    type = str(media['type']).replace("\'", "\\'")
                    sizes = 'None'
                    indices = str(media['indices']).replace("\'", "\\'")
                    url = str(media['url']).replace("\'", "\\'")
                    media_url = str(media['media_url']).replace("\'", "\\'")
                    display_url = str(media['display_url']).replace("\'", "\\'")
                    m_id = str(media['id']).replace("\'", "\\'")
                    m_id_str = str(media['id_str']).replace("\'", "\\'")
                    expanded_url = str(media['expanded_url']).replace("\'", "\\'")
                    media_url_https = str(media['media_url_https']).replace("\'", "\\'")

                    media_stmts.append(
                        "INSERT IGNORE into media VALUES ({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');".format(
                            id, type, sizes, indices, url, media_url, display_url, m_id, m_id_str, expanded_url,
                            media_url_https))

    return tweet_stmts, user_stmts, hashtags_stmts, media_stmts, user_mention_stmts, url_stmts, symbols_stmts


def get_normalized_bulk_insert_statements(path=DOCUMENT):


    tweet_values=""
    user_values=""
    media_values=""
    user_mention_values=""
    url_values=""
    symbol_values=""
    hashtag_values=""


    document = io.open(path, 'r')

    with document as json_docs:
        for data in json_docs:

            data = json.loads(data)

            # tweet

            created_at = str(data['created_at'])
            t_id = str(data['id'])
            t_id_str = str(data['id_str'])
            text = str(data['text']).replace("'", "\\'")
            source = str(data['source']).replace("\'", "\\'")
            truncated = str(data['truncated']).replace("\'", "\\'")
            in_reply_to_status_id = str(data['in_reply_to_status_id']).replace("\'", "\\'")
            in_reply_to_status_id_str = str(data['in_reply_to_status_id_str']).replace("\'", "\\'")
            in_reply_to_user_id = str(data['in_reply_to_user_id']).replace("\'", "\\'")
            in_reply_to_user_id_str = str(data['in_reply_to_user_id_str']).replace("\'", "\\'")
            in_reply_to_screen_name = str(data['in_reply_to_screen_name']).replace("\'", "\\'")

            if in_reply_to_user_id == 'None': in_reply_to_user_id = 0
            if in_reply_to_status_id == 'None': in_reply_to_status_id = 0

            user_id = str(data['user']['id']).replace("\'", "\\'")
            quote_count = str(data['quote_count']).replace("\'", "\\'")
            reply_count = str(data['reply_count']).replace("\'", "\\'")
            favorite_count = str(data['favorite_count']).replace("\'", "\\'")
            favorited = str(data['favorited']).replace("\'", "\\'")
            retweeted = str(data['retweeted']).replace("\'", "\\'")
            filter_level = str(data['filter_level']).replace("\'", "\\'")
            lang = str(data['lang']).replace("\'", "\\'")
            tweet_coordinates = data['coordinates']['coordinates'] if 'type' in data else None
            tweet_coordinates_type = data['coordinates']['type'] if 'type' in data else None
            place_country = str(data['place']['country']) if 'country' in data else None
            place_country_code = str(data['place']['country_code']) if 'country' in data else None
            place_full_name = str(data['place']['full_name']) if 'country' in data else None
            place_id = str(data['place']['id']) if 'country' in data else 0
            place_name = str(data['place']['name']) if 'country' in data else None
            place_place_type = str(data['place']['place_type']) if 'country' in data else None
            place_url = str(data['place']['url']) if 'country' in data else None
            quoted_status_id = str(data['quoted_status_id']) if 'quoted_status_id' in data else 0
            quoted_status_id_str = str(data['quoted_status_id_str']) if 'quoted_status_id' in data else None
            quoted_status = str(data['quoted_status']['text']).replace("\'", "\\'") if 'quoted_status' in data else None
            possibly_sensitive = str(data['possibly_sensitive']) if 'possibly_sensitive' in data else None
            retweeted_status = str(data['retweeted_status']['id']) if 'retweeted_status' in data else None

            try:
                media_id = str(data['entities']['media'][0]['id'])
            except:
                media_id = 0

            try:
                user_mentions_id = str(data['entities']['user_mentions'][0]['id'])
            except:
                user_mentions_id = 0

            try:
                urls_id = str(data['entities']['urls'][0]['id'])
            except:
                urls_id = 0

            try:
                hashtags_id = str(data['entities']['hashtags'][0]['id'])
            except:
                hashtags_id = 0

            try:
                symbols_id = str(data['entities']['symbols'][0]['id'])
            except:
                symbols_id = 0

            tweet_values +="('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', " \
                            "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', " \
                            "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'),".format(
                created_at, t_id, t_id_str, text, source, truncated, in_reply_to_status_id, in_reply_to_status_id_str,
                in_reply_to_user_id, in_reply_to_user_id_str, in_reply_to_screen_name, user_id, tweet_coordinates,
                tweet_coordinates_type, place_country, place_country_code, place_full_name, place_id, place_name,
                place_place_type, place_url, quote_count, reply_count, favorite_count, favorited, retweeted,
                filter_level, lang, quoted_status_id, quoted_status_id_str, quoted_status, possibly_sensitive,
                retweeted_status, media_id, user_mentions_id, urls_id, hashtags_id, symbols_id
            )

            # user table

            u_id = str(data['user']['id'])
            u_id_str = str(data['user']['id_str'])
            name = str(data['user']['name']).replace("\'", "\\'")
            screen_name = str(data['user']['screen_name']).replace("\'", "\\'")
            location = str(data['user']['location']).replace("\'", "\\'")
            url = str(data['user']['url']).replace("\'", "\\'")
            description = str(data['user']['description']).replace("\'", "\\""")
            description = description.replace("\\", "|")
            translator_type = str(data['user']['translator_type']).replace("\'", "\\'")
            protected = str(data['user']['protected']).replace("\'", "\\'")
            verified = str(data['user']['verified']).replace("\'", "\\'")
            followers_count = str(data['user']['followers_count']).replace("\'", "\\'")
            friends_count = str(data['user']['friends_count']).replace("\'", "\\'")
            listed_count = str(data['user']['listed_count']).replace("\'", "\\'")
            favourites_count = str(data['user']['favourites_count']).replace("\'", "\\'")
            statuses_count = str(data['user']['statuses_count']).replace("\'", "\\'")
            created_at = str(data['user']['created_at']).replace("\'", "\\'")
            utc_offset = str(data['user']['utc_offset']).replace("\'", "\\'")
            time_zone = str(data['user']['time_zone']).replace("\'", "\\'")
            geo_enabled = str(data['user']['geo_enabled']).replace("\'", "\\'")
            lang = str(data['user']['lang']).replace("\'", "\\'")
            contributors_enabled = str(data['user']['contributors_enabled']).replace("\'", "\\'")
            is_translator = str(data['user']['is_translator']).replace("\'", "\\'")
            profile_background_color = str(data['user']['profile_background_color']).replace("\'", "\\'")
            profile_background_image_url = str(data['user']['profile_background_image_url']).replace("\'", "\\'")
            profile_background_image_url_https = str(data['user']['profile_background_image_url_https']).replace("\'", "\\'")
            profile_background_tile = str(data['user']['profile_background_tile']).replace("\'", "\\'")
            profile_image_url = str(data['user']['profile_image_url']).replace("\'", "\\'")
            profile_image_url_https = str(data['user']['profile_image_url_https']).replace("\'", "\\'")
            profile_link_color = str(data['user']['profile_link_color']).replace("\'", "\\'")
            profile_sidebar_border_color = str(data['user']['profile_sidebar_border_color']).replace("\'", "\\'")
            profile_sidebar_fill_color = str(data['user']['profile_sidebar_fill_color']).replace("\'", "\\'")
            profile_text_color = str(data['user']['profile_text_color']).replace("\'", "\\'")
            profile_use_background_image = str(data['user']['profile_use_background_image']).replace("\'", "\\'")
            default_profile = str(data['user']['default_profile']).replace("\'", "\\'")
            default_profile_image = str(data['user']['default_profile_image']).replace("\'", "\\'")
            following = str(data['user']['default_profile_image']).replace("\'", "\\'")
            follow_request_sent = str(data['user']['follow_request_sent']).replace("\'", "\\'")
            notifications = str(data['user']['notifications']).replace("\'", "\\'")

            user_values += "('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', " \
                              "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'" \
                              "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'),".format(
                u_id, u_id_str, name, screen_name, location, url, description, translator_type, protected,
                verified, followers_count, friends_count, listed_count, favourites_count, statuses_count,
                created_at, utc_offset, time_zone, geo_enabled, lang, contributors_enabled, is_translator,
                profile_background_color, profile_background_image_url, profile_background_image_url_https,
                profile_background_tile, profile_image_url, profile_image_url_https, profile_link_color,
                profile_sidebar_border_color, profile_sidebar_fill_color, profile_text_color,
                profile_use_background_image, default_profile, default_profile_image, following, follow_request_sent,
                notifications)

            # hashtag

            hashtag_text = data['entities']['hashtags']

            for hashtag in hashtag_text:
                id = data['id']
                hashtag_text = str(hashtag['text']).replace("\'", "\\'")
                indices_text = str(hashtag['indices']).replace("\'", "\\'")


                hashtag_values += "({}, '{}', '{}'),".format(id, hashtag_text, indices_text)

            # urls

            url_text = data['entities']['urls']

            for url in url_text:
                id = data['id']
                url_text = str(url['url'])
                display_url_text = str(url['display_url']).replace("\'", "\\'")
                expanded_url_text = str(url['expanded_url']).replace("\'", "\\'")
                indices_text = str(url['indices']).replace("\'", "\\'")


                url_values += "({}, '{}', '{}', '{}', '{}'),".format(id, url_text, display_url_text, expanded_url_text, indices_text)
            # symbols

            symbol_text = data['entities']['symbols']

            for symbol in symbol_text:
                id = 0
                symbol_text = str(symbol['text']).replace("\'", "\\'")
                indices_text = str(symbol['indices']).replace("\'", "\\'")


                symbol_values += "({}, '{}', '{}'),".format(id, symbol_text, indices_text)
            # user mentions
            user_mention_text = data['entities']['user_mentions']

            for user_mention in user_mention_text:
                id = data['id']
                name = str(user_mention['name']).replace("\'", "\\'")

                # print(name.replace("\\","|"))

                indices = str(user_mention['indices'])
                screen_name = str(user_mention['screen_name']).replace("\'", "\\'")
                u_id = str(user_mention['id']).replace("\'", "\\'")
                u_id_str = str(user_mention['id_str']).replace("\'", "\\'")



                user_mention_values += "({}, '{}','{}','{}','{}','{}'),".format(id, name, indices,screen_name, u_id,
                                                                                                     u_id_str)

            # media

            if 'media' in data['entities']:

                media_text = data['entities']['media']

                for media in media_text:
                    id = data['id']
                    type = str(media['type']).replace("\'", "\\'")
                    sizes = 'None'
                    indices = str(media['indices']).replace("\'", "\\'")
                    url = str(media['url']).replace("\'", "\\'")
                    media_url = str(media['media_url']).replace("\'", "\\'")
                    display_url = str(media['display_url']).replace("\'", "\\'")
                    m_id = str(media['id']).replace("\'", "\\'")
                    m_id_str = str(media['id_str']).replace("\'", "\\'")
                    expanded_url = str(media['expanded_url']).replace("\'", "\\'")
                    media_url_https = str(media['media_url_https']).replace("\'", "\\'")


                    media_values += "({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'),".format(
                            id, type, sizes, indices, url, media_url, display_url, m_id, m_id_str, expanded_url,
                            media_url_https)

    sql_tweets = "INSERT IGNORE INTO tweets VALUES {}".format(tweet_values)
    sql_users =  "INSERT IGNORE INTO users VALUES {}".format(user_values)
    sql_user_mentions = "INSERT IGNORE INTO user_mentions VALUES {}".format(user_mention_values)
    sql_hashtags = "INSERT IGNORE INTO hashtags VALUES {}".format(hashtag_values)
    sql_urls = "INSERT IGNORE INTO urls VALUES {}".format(url_values)
    sql_media = "INSERT IGNORE INTO media VALUES {}".format(media_values)
    sql_symbols = "INSERT IGNORE INTO symbols VALUES {}".format(symbol_values)

    return sql_tweets, sql_users,  sql_hashtags, sql_media, sql_user_mentions, sql_urls,  sql_symbols


def simulation():
    connector = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    cursor = connector.cursor()

    languages = ['es', 'en', 'ru']
    truncated = ['true', 'false']
    id = 0

    for i in range(len(languages)):
        sql_find = "SELECT * FROM `universal` WHERE `tweets.lang` = '{}';".format(
            languages[random.randrange(0, len(languages))])
        cursor.execute(sql_find)
        sql_update = "UPDATE `universal` SET `tweets.truncated` = '{}' WHERE `tweets.truncated` ='{}';".format(
            truncated[random.randrange(0, 1)], truncated[random.randrange(0, 1)])
        cursor.execute(sql_update)
        id += 1
        sql_insert = "INSERT INTO universal (`tweets.created_at`, `tweets.id`, `tweets.id_str`, `tweets.text`,`tweets.truncated`) VALUES ('test', {}, '{}','test', 'false');".format(
            id, id)
        cursor.execute(sql_insert)
