# -*- coding: utf-8 -*-
"""
MySQL Database Driver

    description

author: Adam Edelberg
"""
import io
import json
import logging
import time

import os
import pymysql
import mysql.connector
import config

logger = logging.getLogger(__name__)

USER = config.username
PASS = config.password
HOST = config.mysql_host
PORT = config.mysql_port
DATABASE = config.database
DOCUMENT = config.document
DOCUMENT_SINGLE = config.document_single


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
    except mysql.connector.errorcode as code:
        logger.info("CONNECTION FAILED! ERRO: {}".format(code))
    return client

# untested
def create_schema():
    client = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    cursor = client.cursor()

    f = open('/schema/schema.sql', 'r')

    try:
        sql = f.read()
        cursor.execute(sql)

    finally:
        f.close()


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
    except Exception as err:
        print(err)
    client.commit()
    cursor.close()
    client.close()


def scan_all():
    """Scan from a table (default = universal)

    Parameters:
        table - an existing sql table in benchmark_db

    Returns:

    """

    # conn = mysql.connector.connect(user=u, password=p, host=h)

    conn = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    cursor = conn.cursor()

    sql = "SELECT * FROM universal;"

    start = time.time()
    cursor.execute(sql)
    run = time.time() - start

    cursor.fetchall()
    count = cursor.rowcount

    logger.info(" %s seconds to scan %d objects from universal table", run, count)

    return run, count


def universal_select_with_indexing():
    conn = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    cursor = conn.cursor()

    sql1 = "SELECT COUNT(*) from universal_indexed where `users.location` = 'London';"
    sql2 = "SELECT COUNT(*) FROM universal_indexed WHERE `users.friends_count`>1000;"
    sql3 = "SELECT COUNT(*) FROM universal_indexed WHERE `users.followers_count`>1000;"

    num = 0
    sql_time = 0

    for i in range(5):
        start = time.time()
        cursor.execute(sql1)
        sql_time += time.time() - start

        res = cursor.fetchone()
        for row in res: num=row

    for i in range(5):
        start = time.time()

        cursor.execute(sql2)
        sql_time += time.time() - start

        res = cursor.fetchone()
        for row in res: num = row

    for i in range(5):
        start = time.time()

        cursor.execute(sql3)
        sql_time += time.time() - start

        res = cursor.fetchone()
        for row in res: num = row

    logger.info("{} seconds to select {} objects indexed".format(sql_time, num))
    return sql_time


def universal_select(indexed, doc_path):
    conn = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    cursor = conn.cursor()

    if indexed:
        sql1 = "SELECT COUNT(*) from universal_indexed where `users.location` = 'London';"
        sql2 = "SELECT COUNT(*) FROM universal_indexed WHERE `users.friends_count`>1000;"
        sql3 = "SELECT COUNT(*) FROM universal_indexed WHERE `users.followers_count`>1000;"
    else:
        sql1 = "SELECT COUNT(*) from universal WHERE `users.location` = 'London';"
        sql2 = "SELECT COUNT(*) FROM universal WHERE `users.friends_count`>1000;"
        sql3 = "SELECT COUNT(*) FROM universal WHERE `users.followers_count`>1000;"

    num = 0
    sql_time = 0

    for i in range(5):
        start = time.time()
        cursor.execute(sql1)
        sql_time += time.time() - start
        res = cursor.fetchone()
        for row in res: num=row

    for i in range(5):
        start = time.time()
        cursor.execute(sql2)
        sql_time += time.time() - start
        res = cursor.fetchone()
        for row in res: num = row

    for i in range(5):
        start = time.time()
        cursor.execute(sql3)
        sql_time += time.time() - start
        res = cursor.fetchone()
        for row in res: num = row

    size = "{}MB".format(round(os.path.getsize(doc_path) / 1024 / 1024, 2))
    logger.info("{} seconds to select {} objects indexed={}, doc_size={}".format(sql_time, num, indexed,size))

    return sql_time, size


def bulk_insert_normalized():
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
    sql = 'SET NAMES utf8mb4;'
    cursor.execute(sql)

    tweet_stmts, user_stmts, hashtags_stmts, media_stmts, user_mention_stmts, url_stmts, symbols_stmts = get_normalized_statements()

    run = 0

    for sql in tweet_stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(sql)
            pass
        run += time.time() - start
    for sql in user_stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(sql)
            pass
        run += time.time() - start
    for sql in hashtags_stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(sql)

            pass
        run += time.time() - start
    for sql in media_stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(sql)

            pass
        run += time.time() - start
    for sql in user_mention_stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(sql)

            pass
        run += time.time() - start
    for sql in url_stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(sql)

            pass
        run += time.time() - start
    for sql in symbols_stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(sql)

            pass
        run += time.time() - start

    cursor.close()
    conn.commit()
    conn.close()

    size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))
    logger.info("{} seconds to bulk insert normalized {}".format(run, size))
    return run, size


def bulk_insert_universal(doc_path, indexed=False):

    if indexed:
        stmts=get_statements(table='universal_indexed',doc=doc_path)
        delete_from_table(table='universal_indexed')

    else:
        stmts = get_statements(table='universal', doc=doc_path)
        delete_from_table('universal')

    #connector = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)
    connector = mysql.connector.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)

    cursor = connector.cursor()
    sql = 'SET NAMES utf8mb4;'
    cursor.execute(sql)

    run = 0
    for sql in stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            pass
            #print(e)

        run += time.time() - start

    cursor.close()
    connector.commit()
    connector.close()

    size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))
    logger.info("{} seconds to bulk insert universal {}".format(run, size))
    return run, size


def universal_insert_one_with_indexing_2():

    stmts = get_statements(table='universal_indexed')

    delete_from_table(table='universal_indexed')

    connector = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)

    cursor = connector.cursor()
    run = 0
    for sql in stmts:
        try:
            start = time.time()
            cursor.execute(sql)
            run += time.time() - start
        except Exception as e:
            pass

    stmts = get_statements(table='universal_indexed', doc=DOCUMENT_SINGLE)

    start = time.time()
    cursor.execute(stmts.pop())
    run2 = time.time() - start

    cursor.close()
    connector.commit()
    connector.close()

    #logger.info("{} seconds to universal_insert_one_with_indexing".format(run2))
    single_size = "{}MB".format(round(os.path.getsize(DOCUMENT_SINGLE) / 1024 / 1024, 2))

    db_size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))

    logger.info("{} seconds to universal insert one with indexing, db_size={} doc_size={}".format(run2, db_size, single_size))

    return run2, single_size, db_size, run
    #return run2


def universal_insert_one_without_indexing_2():
    stmts = get_statements(table='universal')

    delete_from_table(table='universal')

    connector = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)

    cursor = connector.cursor()
    run=0
    for sql in stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            pass
        run += time.time() - start

    stmts = get_statements(table='universal', doc=DOCUMENT_SINGLE)

    start = time.time()
    cursor.execute(stmts.pop())
    run2 = time.time() - start

    cursor.close()
    connector.commit()
    connector.close()
    single_size = "{}MB".format(round(os.path.getsize(DOCUMENT_SINGLE) / 1024 / 1024, 2))
    db_size = "{}MB".format(round(os.path.getsize(DOCUMENT) / 1024 / 1024, 2))

    #logger.info("{} seconds to universal_insert_one_without_indexing".format(run))
    logger.info("{} seconds to universal insert one without indexing, db_size={} doc_size={}".format(run2, db_size, single_size))

    return run2, single_size, db_size, run

# not used
def bulk_insert_universal_indexed_2():
    stmts = get_statements(table='universal_indexed')

    delete_from_table('universal_indexed')

    connector = pymysql.connect(user=USER, password=PASS, host=HOST, db=DATABASE, autocommit=False)

    cursor = connector.cursor()

    sql = 'SET NAMES utf8mb4;'
    cursor.execute(sql)


    run=0
    for sql in stmts:
        start = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            pass
        run += time.time() - start

    cursor.close()
    connector.commit()
    connector.close()

    return run, len(stmts)


#############################
#  Generate SQL Statements  #
#############################


def get_statements(table, doc=DOCUMENT):
    document = io.open(doc, 'r')

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
            quoted_status = str(data['quoted_status']['text']).replace("\'","\\'") if 'quoted_status' in data else None
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
            time_zone = str(data['user']['time_zone'])
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
                symbol_id = data['id']
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

    return stmts


def get_normalized_statements(doc=DOCUMENT):
    tweet_stmts = []
    user_stmts = []
    media_stmts = []
    user_mention_stmts = []
    url_stmts = []
    symbols_stmts = []
    hashtags_stmts = []

    document = io.open(doc, 'r')

    with document as json_docs:
        for data in json_docs:

            data = json.loads(data)

            # tweet

            created_at = str(data['created_at'])
            t_id = str(data['id'])
            t_id_str = str(data['id_str'])
            text = str(data['text']).replace("'", "\\'")
            source = str(data['source']).replace("\'", "\\'")
            truncated = str(data['truncated'])
            in_reply_to_status_id = str(data['in_reply_to_status_id'])
            in_reply_to_status_id_str = str(data['in_reply_to_status_id_str'])
            in_reply_to_user_id = str(data['in_reply_to_user_id'])
            in_reply_to_user_id_str = str(data['in_reply_to_user_id_str'])
            in_reply_to_screen_name = str(data['in_reply_to_screen_name'])

            if in_reply_to_user_id == None: in_reply_to_user_id = 0
            if in_reply_to_status_id == None: in_reply_to_status_id = 0

            user_id = str(data['user']['id'])
            quote_count = str(data['quote_count'])
            reply_count = str(data['reply_count'])
            favorite_count = str(data['favorite_count'])
            favorited = str(data['favorited'])
            retweeted = str(data['retweeted'])
            filter_level = str(data['filter_level'])
            lang = str(data['lang'])
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
            quoted_status = str(data['quoted_status']['text']).replace("\'","\\'") if 'quoted_status' in data else None
            possibly_sensitive = str(data['possibly_sensitive']) if 'possibly_sensitive' in data else None
            retweeted_status = str(data['retweeted_status']['id']) if 'retweeted_status' in data else None

            # note the IGNORE here - there might be duplicate tweets in the data source

            tweet_stmts.append("INSERT IGNORE INTO tweets VALUES (" \
                               "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', " \
                               "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', " \
                               "'{}', '{}', '{}', '{}', '{}', '{}', '{}');".format(
                created_at, t_id, t_id_str, text, source, truncated, in_reply_to_status_id, in_reply_to_status_id_str,
                in_reply_to_user_id, in_reply_to_user_id_str, in_reply_to_screen_name, user_id, tweet_coordinates,
                tweet_coordinates_type, place_country, place_country_code, place_full_name, place_id, place_name,
                place_place_type, place_url, quote_count, reply_count, favorite_count, favorited, retweeted,
                filter_level, lang, quoted_status_id, quoted_status_id_str, quoted_status, possibly_sensitive,
                retweeted_status
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
            protected = str(data['user']['protected'])
            verified = str(data['user']['verified'])
            followers_count = str(data['user']['followers_count'])
            friends_count = str(data['user']['friends_count'])
            listed_count = str(data['user']['listed_count'])
            favourites_count = str(data['user']['favourites_count'])
            statuses_count = str(data['user']['statuses_count'])
            created_at = str(data['user']['created_at'])
            utc_offset = str(data['user']['utc_offset'])
            time_zone = str(data['user']['time_zone'])
            geo_enabled = str(data['user']['geo_enabled'])
            lang = str(data['user']['lang'])
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
                hashtag_text = str(hashtag['text'])
                indices_text = str(hashtag['indices'])

                hashtags_stmts.append(
                    "INSERT IGNORE into hashtags (id, hashtag, indices) VALUES ({}, '{}', '{}');".format(
                        id, hashtag_text, indices_text))

            # urls

            url_text = data['entities']['urls']

            for url in url_text:
                id = data['id']
                url_text = str(url['url'])
                display_url_text = str(url['display_url'])
                expanded_url_text = str(url['expanded_url'])
                indices_text = str(url['indices'])

                url_stmts.append("INSERT IGNORE into urls VALUES ({}, '{}', '{}', '{}', '{}');".format(
                    id, url_text, display_url_text, expanded_url_text, indices_text))

            # symbols

            symbol_text = data['entities']['symbols']

            for symbol in symbol_text:
                id = data['id']
                symbol_text = str(symbol['text'])
                indices_text = str(symbol['indices'])

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
                u_id = str(user_mention['id'])
                u_id_str = str(user_mention['id_str'])

                user_mention_stmts.append(
                    "INSERT IGNORE into user_mentions VALUES ({}, '{}','{}','{}','{}','{}');".format(id, name, indices,
                                                                                                     screen_name, u_id,
                                                                                                     u_id_str))

            # media

            if 'media' in data['entities']:

                media_text = data['entities']['media']

                for media in media_text:
                    id = data['id']
                    type = str(media['type'])
                    sizes = 'None'
                    indices = str(media['indices'])
                    url = str(media['url'])
                    media_url = str(media['media_url'])
                    display_url = str(media['display_url'])
                    m_id = str(media['id'])
                    m_id_str = str(media['id_str'])
                    expanded_url = str(media['expanded_url'])
                    media_url_https = str(media['media_url_https'])

                    media_stmts.append(
                        "INSERT IGNORE into media VALUES ({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');".format(
                            id, type, sizes, indices, url, media_url, display_url, m_id, m_id_str, expanded_url,
                            media_url_https))

    return tweet_stmts, user_stmts, hashtags_stmts, media_stmts, user_mention_stmts, url_stmts, symbols_stmts

