
#  [DPD: Database Performance Driver Docs v1](github.com/driverport) ![CI status](https://img.shields.io/badge/build-passing-brightgreen.svg)

DPD is a database performance benchmarking tool used to test the performance throughput MongoDB and MySQL. 

# Prerequisites


The following libraries are required to be installed on **Python 3.6** (or higher):
* PyMySQL (version X or higher)
* PyMongo (version X or higher)

Instances of MongoDB and MySQL are **required to be running** before any tests can be performed. 

* Installation instructions for  MongoDB are availible at: 			https://docs.mongodb.com/manual/installation/

* MongoDB Sharding instructions are availible at: 
https://docs.mongodb.com/manual/tutorial/deploy-shard-cluster

* Installation instructions for MySQL are availible at: 
https://dev.mysql.com/doc/en/installing.html


# Instructions

## Configuring database settings and datasets

The config.py class is used to hold global database settings. Here, network and access control variables are defined. The program uses default host and port numbers for connecting to the databases. 

Datasets paths are also configured in this class. **Please note that the databases expect line-delimited JSON documents for insertion.**

## Creating a Twitter dataset

Any Twitter can be used with the current database configuration. Should you wish to use your own dataset, the data needs to be parsed. The json_tools.py class can be used to clean datasets and create custom length datafiles for testing. 

The following example illustrate how to create a dataset of 10 000 records from two Twitter JSON documents:

	python3 json_tools --join <file1> <file2> ...

To create a 10 000 record dataset:
	
	python3 json_tools --create_space_delimited 10000

## Performance Testing

To perform database testing the following attributes are defined*: 


	1. Database				choices={0 = MongoDB, 1 = MySQL}
	2. Target Test				choices={use number - see below}
	3. Number of Threads			choices={<50 **} 
	4. Indexed Flag				choices={True/False} 
	5. Number of Test Iterations		choices={integer}
	6. Simulation Flag			choices={True/False} 
	7. Debugger Verbosity			choices={v, vv, vvv} (increasing verbosity)

***NOTE**: Ommitting attributes will result in using DEFAULT settings. DEFAULTS are specified: database=0, test=0, threads=4, indexed=False, iterations=2, simulation=False, debugger=vv

**Figure is dependent on physical architecture (more than 50 threads has not been tested and may yeild unexpected results).

### The target tests **numbers** are defined as follows: 
|MongoDB|MySQL|
|--|--|
|**0:** bulk_insert|**1:** bulk_insert_universal|
|**2:** bulk_insert_collections|**3:** bulk_insert_normalized|
|**4:** bulk_insert_one|**5:** insert_one_universal|
|**6:** bulk_insert_one_collections|**7:** insert_one_normalized|
|**8:** insert_one|**9:** insert_one_universal
|**10:** insert_one_collections|**11:** insert_one_normalized|
|**12:** find|**13:** select_universal
|**14:** find_collections|**15:** select_normalized 
|**16:** scan|**17:** scan_universal
|**18:** scan_collections|**19:** scan_normalized


## Logging Test Results
All tests are automatically logged to a csv file in the directory where the code is invoked. This file is an appended log file so previous test results will note be overridden. Log times are recorded in **seconds** for all tests.


## Sample Code Execution

An example of running a parametized MongoDB bulk-insert test is illustrated below with an expected output:

	python3 main.py --database 0 --test 0 --iterations 10 --indexed False --simulated True --threads 5 --debug vv

Expected Output:
	
	INFO: 2018-09-19 12:49:07,024: mongo_db: 0.6544959545135498 seconds to bulk_insert 4.89MB, indexed=False
	INFO: 2018-09-19 12:49:08,078: mongo_db: 0.6548271179199219 seconds to bulk_insert 4.89MB, indexed=False
	INFO: 2018-09-19 12:49:09,083: mongo_db: 0.6771829128265381 seconds to bulk_insert 4.89MB, indexed=False
	INFO: 2018-09-19 12:49:10,091: mongo_db: 0.6575758457183838 seconds to bulk_insert 4.89MB, indexed=False
	INFO: 2018-09-19 12:49:11,158: mongo_db: 0.6706907749176025 seconds to bulk_insert 4.89MB, indexed=False
	INFO: 2018-09-19 12:49:12,184: mongo_db: 0.6512908935546875 seconds to bulk_insert 4.89MB, indexed=False
	INFO: 2018-09-19 12:49:13,320: mongo_db: 0.7920939922332764 seconds to bulk_insert 4.89MB, indexed=False
	INFO: 2018-09-19 12:49:14,195: mongo_db: 0.6045279502868652 seconds to bulk_insert 4.89MB, indexed=False
	INFO: 2018-09-19 12:49:15,175: mongo_db: 0.6678547859191895 seconds to bulk_insert 4.89MB, indexed=False
	INFO: 2018-09-19 12:49:16,917: mongo_db: 0.9594857692718506 seconds to bulk_insert 4.89MB, indexed=False

	mongo_db.bulk_insert: doc_size=4.89MB, time_mean=0.6990025997161865

	INFO: 2018-09-19 12:49:16,947: root: Stopping Thread-0
	INFO: 2018-09-19 12:49:16,948: root: Stopping Thread-3
	INFO: 2018-09-19 12:49:16,956: root: Stopping Thread-1
	INFO: 2018-09-19 12:49:16,957: root: Stopping Thread-4
	INFO: 2018-09-19 12:49:16,975: root: Stopping Thread-2

# License
This work is licensed under the [GNU General Public License v2.0.](https://choosealicense.com/licenses/gpl-3.0/) 
