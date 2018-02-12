import os
import shutil
import random

from database import ProjectUtils, ProjectReader, DatasetUtils, DatasetReader
import MySQLdb as mysql


# Test database utilities for keeping track of files.

generate=False

def main():
    drop_all_tables()
    clean_test_area()
    initialize_master_tables()
    generate_project1()
    read_project1()
    generate_project2()
    # red_project2()

def clean_test_area():
    shutil.rmtree('/data/test/test_1')
    shutil.rmtree('/data/test/test_2')

def initialize_master_tables():

    dataset_master_index_sql = """
        CREATE TABLE IF NOT EXISTS dataset_master_index (
            id          INTEGER     NOT NULL AUTO_INCREMENT,
            dataset     VARCHAR(50) NOT NULL UNIQUE,
            created     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
            modified    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        ); """

    dataset_master_consumption_sql = """
        CREATE TABLE IF NOT EXISTS dataset_master_consumption (
            id     INTEGER  NOT NULL AUTO_INCREMENT,
            input  INTEGER  NOT NULL,
            output INTEGER  NOT NULL,
            FOREIGN KEY(input)  REFERENCES dataset_master_index(id) ON UPDATE CASCADE,
            FOREIGN KEY(output) REFERENCES dataset_master_index(id) ON UPDATE CASCADE,
            PRIMARY KEY (id)
        ); """

    host = 'localhost'
    username = 'guenettedb_admin'
    password = 'admin'
    conn = mysql.connect(host=host,                 # your host, usually localhost
                     user=username,             # your username
                     passwd=password,           # your password
                     db='guenette_neutrinos',   # name of the data base
                     autocommit=False)          # Prevent automatic commits
    cur = conn.cursor()

    try:
        cur.execute(dataset_master_index_sql)
    except Error as e:
        print e
        print "Could not create master index table"
    try:
        cur.execute(dataset_master_consumption_sql)
    except Error as e:
        print "Could not create master consumption table"

    print "Initialization complete."

def drop_all_tables():

    host = 'localhost'
    username = 'guenettedb_admin'
    password = 'admin'
    conn = mysql.connect(host=host,                 # your host, usually localhost
                     user=username,             # your username
                     passwd=password,           # your password
                     db='guenette_neutrinos',   # name of the data base
                     autocommit=False)          # Prevent automatic commits
    cur = conn.cursor()

    cur.execute('SHOW tables;')
    tables = cur.fetchall()
    if ('dataset_master_consumption', ) in tables:
        try:
            cur.execute('DROP TABLE dataset_master_consumption;')
        except:
            pass
    if ('dataset_master_index', ) in tables:
        try:
            cur.execute('DROP TABLE dataset_master_index;')
        except:
            pass


    cur.execute('SHOW tables;')
    tables = cur.fetchall()
    while len(tables) > 0:
        for table in reversed(tables):

            sql = '''DROP TABLE {0};'''.format(table[0])
            print sql
            cur.execute(sql)
        cur.execute('SHOW tables;')
        tables = cur.fetchall()


    cur.execute('SHOW tables;')
    tables = cur.fetchall()
    if ('dataset_master_consumption', ) in tables:
        try:
            cur.execute('DROP TABLE dataset_master_consumption;')
        except:
            pass
    if ('dataset_master_index', ) in tables:
        try:
            cur.execute('DROP TABLE dataset_master_index;')
        except:
            pass

def generate_project2():

    proj_util = ProjectUtils()
    dataset_util = DatasetUtils()

    print proj_util.list_datasets()

    print proj_util.create_dataset(dataset="test_2", parents=['test_1'])

    os.mkdir('/data/test/test_2')
    top_dir = '/data/test/test_2/'

    # Fake these files:
    for i in xrange(5):
        job_id = "23456_{0}".format(i)

        job_dir = top_dir + "{0}_{1}".format(job_id, i)
        os.mkdir(job_dir)
        f_name = job_dir + "/empty_file_2_{0}.txt".format(i)
        input_files = dataset_util.yield_files('test_2', n=2, jobid=job_id)

        if (random.randint(1,5) < 2):
            continue

        N = 0
        with open(f_name, 'w') as output:
            for input_file in input_files:
                with open(input_file, 'r') as input_f:
                    for line in input_f.readlines():
                        output.write(line)
                        N += 1


        f_size = os.path.getsize(f_name)
        print job_id
        _id = dataset_util.declare_file(dataset = 'test_2',
                                        filename = f_name,
                                        run = 0,
                                        ftype = 0,
                                        nevents = N,
                                        jobid = job_id,
                                        size = f_size)

        # Finish declaring the consumed files:
        dataset_util.consume_files('test_2', jobid=job_id, output_file_id = _id)

    dataset_util.reset_consumption_table('test_2')

def generate_project1():

    proj_util = ProjectUtils()
    dataset_util = DatasetUtils()

    proj_util.create_dataset(dataset="test_1", parents=None)

    os.mkdir('/data/test/test_1')

    # Create some files for this project:
    top_dir = '/data/test/test_1/'
    for i in xrange(10):
        job_id = 12345
        job_dir = top_dir + "{0}_{1}".format(job_id, i)
        os.mkdir(job_dir)
        f_name = job_dir + "/empty_file_{0}.txt".format(i)
        N = random.randint(1,100)
        with open(f_name, 'w') as _f:
            for n in xrange(N):
                _f.write("blah")
        f_size = os.path.getsize(f_name)
        print f_name
        dataset_util.declare_file(dataset = 'test_1',
                                  filename = f_name,
                                  run = 0,
                                  ftype = 0,
                                  nevents = N,
                                  jobid = job_id,
                                  size = f_size)

    # Test informational functions about this dataset:

    # print proj_util.dataset_ids('test_1')
    # print ""
    # print proj_util.dataset_ids(['test_1'])
    # print ""

    # proj_util.drop_dataset(dataset="test_1")

    # for i in xrange(5)


def read_project1():
    proj_reader = ProjectReader()
    data_reader = DatasetReader()

    # Looking for files for dataset 1 'test_1'

    print "Dataset ID for test_1: "+ str(proj_reader.dataset_ids('test_1'))

    print data_reader.count_files(dataset='test_1')
    print data_reader.count_files(dataset='test_1', run=1)
    print data_reader.count_files(dataset='test_1', type=0)
    print data_reader.count_files(dataset='test_1', filename='/data/test/test_1/12345_0/empty_file_0.txt')

    print "Total events: " + str(data_reader.sum(dataset='test_1', target='nevents'))
    print "Total size: "   + str(data_reader.sum(dataset='test_1', target='size'))

if __name__ == '__main__':
    main()