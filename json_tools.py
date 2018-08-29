# -*- coding: utf-8 -*-
"""
JSON Parsing Tools

    This module contains all the parsing and test file creation tools used in these experiments.
    The functions below assume that JSON docs data delimited by \n or newline characters in a single file.

    Dependencies:
        argparse
        json
        shutil

"""
import argparse
import json
import shutil

JSON_SRC = '../raw_data/big.json'


def raw_json(documents):
    """Load JSON documents that have been stored in a single file.

    This method is also the place to clean incoming JSON docs should they not be valid types.
    In the case below, each if statement checks the parse for illegitimate/unwanted JSON docs.

    Parameters:
        documents

    Returns:
        JSON object type
    """

    json_text = ""

    for doc in documents:

        json_text += doc

        # throw away unrelated data
        if '{"limit":{"track":' in json_text:
            json_text = ""

        if '"display_text_range":[' in json_text:
            json_text = ""

        # parse line as a JSON document
        try:
            yield json.loads(json_text)
            json_text = ""
        except ValueError:
            pass


# Create JSON docs newline
def create_docs(lines, indent, sort, json_dst):

    source = '../raw_data/big.json'
    data_parsed = "../parsed_data/{}-{}.json".format(source[12:-5], lines)

    # for making space and comma delimited json files

    count = 0

    dst = open(data_parsed, 'w')

    while count < lines:
        with open(JSON_SRC, 'r') as src:

            for doc in raw_json(src):

                dst.write(json.dumps(doc, indent=indent, sort_keys=sort))
                dst.write('\n')

                count += 1
                print('created {} docs'.format(count))
                if count == lines:
                    dst.close()
                    break


# Create JSON docs dictionary
def create_docs_d(lines, indent, sort):
    source = '../raw_data/big.json'
    data_parsed_dict = "../parsed_data/{}-{}d.json".format(source[12:-5], lines)

    # for making space and comma delimited json files

    count = 1

    dst_dict = open(data_parsed_dict, 'w')

    dst_dict.write('[')

    loop = True
    while loop:
        with open(JSON_SRC, 'r') as src:
            for doc in raw_json(src):

                if count == (lines-1):
                    print('created {} docs'.format(count))
                    dst_dict.write(json.dumps(doc, indent=indent, sort_keys=sort))
                    count += 1

                elif count == lines:
                    dst_dict.write(']')
                    print('created {} docs'.format(count))
                    loop=False
                    break

                else:
                    dst_dict.write(json.dumps(doc, indent=indent, sort_keys=sort))
                    dst_dict.write(',\n')
                    count += 1
                    print(count)
    dst_dict.close()


# Efficient file joiner for making big JSONs
def join_files():
    with open('../big.json', 'wb') as wfd:
        for f in ['../raw_data/Eurovision3.json',
                  '../raw_data/Eurovision4.json',
                  '../raw_data/Eurovision5.json',
                  '../raw_data/Eurovision6.json',
                  '../raw_data/Eurovision7.json',
                  '../raw_data/Eurovision8.json',
                  '../raw_data/Eurovision10.json']:
            with open(f, 'rb') as fd:
                shutil.copyfileobj(fd, wfd, 1024 * 1024 * 10)

                # copying in 10mb chunks


# TODO: finish proper argument
parser = argparse.ArgumentParser(description='json_tools module')
parser.add_argument('-j', '--join', help='Join two json documents.', required=False)
parser.add_argument('-cs', '--create_space_delimited', help='Create space delimited json document.', required=False)
parser.add_argument('-cc', '--create_comma_delimited', help='Create comma delimited json document.', required=False)
args = parser.parse_args()

if __name__ == '__main__':
    print("Creating...")
    # 135?
    create_docs(540000,None,False)
    create_docs_d(540000,None,False)

    # join_files()

    print("Done!")
