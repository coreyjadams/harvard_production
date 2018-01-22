import os
from database import DBUtil

# Test database utilities for keeping track of files.

generate=False

def main():
    fake_db(fname='/data/test/output.db')

def fake_db(fname):
    # Declare an output database first and pretend to add files to it:

    test_db = DBUtil(fname)

    # Create some fake files and declare them to the database:
    if generate:
        for i in xrange(5):
            _ = open('/data/test/output/test_file_{}.root'.format(i), 'w')
            print test_db.declare_file(filename="test_file_{}.root".format(i),
                                location='/data/test/output/',
                                stage='test',
                                status=0,
                                nevents=10,
                                ftype=0)
            _ = open('/data/test/output/test_file_failed_{}.root'.format(i), 'w')
            print test_db.declare_file(filename="test_file_failed_{}.root".format(i),
                                location='/data/test/output/',
                                stage='test',
                                status=2,
                                nevents=10,
                                ftype=0)
            _ = open('/data/test/output/test_file_running_{}.root'.format(i), 'w')
            print test_db.declare_file(filename="test_file_running_{}.root".format(i),
                                location='/data/test/output/',
                                stage='test',
                                status=1,
                                nevents=10,
                                ftype=0)
            _ = open('/data/test/output/test_file_{}_ana.root'.format(i), 'w')
            print test_db.declare_file(filename="test_file_{}_ana.root".format(i),
                                location='/data/test/output/',
                                stage='test',
                                status=0,
                                nevents=10,
                                ftype=1)

    print "Counting number of events of type 0:"
    print test_db.count_events(stage=None, ftype=0, status=0)

    print "Counting number of events of type 0:"
    print test_db.count_files(stage=None, ftype=0, status=0)

    print "Requesting 2 files:"
    test_db.list_files(stage='test', ftype=0, status=None, max_n_files=2)
    print "Requesting all files:"
    test_db.list_files(stage='test', ftype=0, status=None)


if __name__ == '__main__':
    main()