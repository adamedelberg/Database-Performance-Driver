# -*- coding: utf-8 -*-
"""
JSON Parsing Tools

    This module contains all the parsing and test file creation tools used in these experiments.
    The functions below assume that JSON docs data delimited by \n or newline characters in a single file.

"""

import json
import shutil

# Load and clean JSON documents
def load_docs(documents):
    json_text = ""
    for doc in documents:
        json_text += doc
        # throw away non-related data lines
        if '{"limit":{"track":' in json_text:
            json_text = ""
        if '"display_text_range":[' in json_text:
            json_text = ""
        try:
            yield json.loads(json_text)
            json_text = ""
        except ValueError:
            pass


# Create JSON docs newline
def create_docs(lines, indent, sort):
    source = '../raw_data/big.json'
    data_parsed = "../parsed_data/e{}-{}.json".format(source[22:-5], lines)

    # for making space and comma delimited json files

    count = 0

    dst = open(data_parsed, 'w')

    while count < lines:
        with open(source, 'r') as src:

            for doc in load_docs(src):

                dst.write(json.dumps(doc, indent=indent, sort_keys=sort))
                dst.write('\n')

                count += 1
                print(count)
                if count == lines:
                    dst.close()
                    break




# Create JSON docs dictionary

def create_docs_d(lines, indent, sort):
    source = '../raw_data/Eurovision10.json'
    data_parsed_dict = "../parsed_data/e{}-{}d.json".format(source[22:-5], lines)

    # for making space and comma delimited json files

    count = 1

    dst_dict = open(data_parsed_dict, 'w')

    dst_dict.write('[')

    loop = True
    while loop:
        with open(source, 'r') as src:
            for doc in load_docs(src):

                if count == (lines-1):
                    print(count)
                    print('1')
                    dst_dict.write(json.dumps(doc, indent=indent, sort_keys=sort))
                    count += 1

                elif count == lines:
                    dst_dict.write(']')
                    print(count)
                    print('2')
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


if __name__ == '__main__':
    print("Creating...")

    create_docs(1500000,None,False)
    create_docs_d(1500000,None,False)

    # join_files()

    print("Done!")
