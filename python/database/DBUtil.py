from itertools import count

import sqlite3
from sqlite3 import Error

#Some of these functions are from the python sqlite tutorial here:
# http://www.sqlitetutorial.net/sqlite-python/

class DBUtil(object):
    """
    Utility class for accessing the file database for a project


    DB columns are:

    Filename - the name of a particular file (str)
    Dataset  - a group of files all created together
    Location - the disk location of that file (str)
    Stage    - the stage that produced that file (str)
    Status   - current status of that file (uint8)
        - 0 == completed
        - 1 == running
        - 2 == failed
        - .....
    NEvents  - number of events in the file
    Type     - 0 for artroot, 1 for analysis, 2 for other
    Consumed - Marker to indicate if the file has been consumed
        - 0 == not consumed
        - 1 == marked for consumption but not yet confirmed (won't be yielded unless makeup==True)
        - 2 == fully consumed, next files confirmed.
    TODO : ParentID - ID (in the table) of the parent file or files
    """

    def __init__(self, db_file):
        super(DBUtil, self).__init__()
        self.db_file = db_file
        default_table = """ CREATE TABLE IF NOT EXISTS files (
                                id integer PRIMARY KEY,
                                dataset text NOT NULL,
                                name text NOT NULL,
                                location text NOT NULL,
                                stage text NOT NULL,
                                status integer NOT NULL,
                                nevents integer NOT NULL,
                                type integer NOT NULL,
                                consumed integer NOT NULL DEFAULT 0
                            ); """
        conn = self.create_connection()
        if conn is not None:
            self.create_table(conn, default_table)
        else:
            raise Exception("Could not create database {0}".format(db_file))


    def file(self):
        return self.db_file

    def create_connection(self):
        """ create a database connection to the SQLite database
            specified by db_file
        :return: Connection object or None
        """
        try:
            conn = sqlite3.connect(self.db_file)
            return conn
        except Error as e:
            print(e)

        return None


    def create_table(self, conn, create_table_sql):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            c = conn.cursor()
            c.execute(create_table_sql)
        except Error as e:
            print(e)


    def dump_all_files(self):
        """
        Query all rows in the tasks table
        :return:
        """
        cur = self.create_connection().cursor()
        cur.execute("SELECT * FROM files")

        rows = cur.fetchall()
        for row in rows:
            print(row)


    def declare_file(self, dataset, filename, location, stage, status, nevents, ftype):
        '''
        Declare a file to the database

        '''

        with  self.create_connection() as conn:

            sql = '''INSERT INTO files(name,dataset,location,stage,status,nevents,type)
                     VALUES(?,?,?,?,?,?,?) '''
            cur = conn.cursor()
            f = (filename, dataset, location, stage, status, nevents, ftype)
            cur.execute(sql, f)


    def count_events(self, dataset, stage, ftype, status):
        '''Count number of declared events as specified


        Arguments:
            stage {[type]} -- [description]
        '''
        if dataset is None:
            raise Exception('Must specify dataset')

        if stage is None and ftype is None and status is None:
            raise Exception('Can not query database without selection')

        where=['dataset=?']
        feed_list=[dataset]
        if stage is not None:
            where     += 'stage=?',
            feed_list += stage,
        if ftype is not None:
            where     +='type=?',
            feed_list += ftype,
        if status is not None:
            where     += 'status=?',
            feed_list += status,


        where = 'WHERE ' + ' AND '.join(where)

        with self.create_connection() as conn:
            cur = conn.cursor()
            sql = '''SELECT SUM(nevents)
                     FROM files
                     {0}
                  '''.format(where)

            cur.execute(sql, feed_list)
            results = cur.fetchone()[0]
        if results is None:
            return 0
        return results


    def count_files(self, dataset, stage, ftype, status):
        '''Count the number of files matching the description

        '''
        if dataset is None:
            raise Exception('Must specify dataset')


        if stage is None and ftype is None and status is None:
            raise Exception('Can not query database without selection')

        where=['dataset=?']
        feed_list=[dataset]
        if stage is not None:
            where     += 'stage=?',
            feed_list += stage,
        if ftype is not None:
            where     +='type=?',
            feed_list += ftype,
        if status is not None:
            where     += 'status=?',
            feed_list += status,


        where = 'WHERE ' + ' AND '.join(where)

        with self.create_connection() as conn:
            cur = conn.cursor()
            sql = '''SELECT COUNT(id)
                     FROM files
                     {0}
                  '''.format(where)

            cur.execute(sql, feed_list)
            results = cur.fetchone()[0]

        return results

    def list_datasets(self):
        '''List all of the datasets in the file

        Select the unique elements and return them

        Arguments:
            dataset {str} -- dataset name
        '''

        sql = '''SELECT DISTINCT dataset
                 FROM files
              '''

        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            results = cur.fetchall()

        return results

    def list_files(self, dataset, stage, ftype, status, max_n_files=-1, select_all=False):
        """
        Query all rows in the tasks table with parameters
        """

        if dataset is None:
            raise Exception('Must specify dataset')


        # Build up a where query:
        if stage is None and ftype is None and status is None:
            raise Exception('Can not query database without selection')

        where=['dataset=?']
        feed_list=[dataset]
        if stage is not None:
            where     += 'stage=?',
            feed_list += stage,
        if ftype is not None:
            where     +='type=?',
            feed_list += ftype,
        if status is not None:
            where     += 'status=?',
            feed_list += status,

        where = 'WHERE ' + ' AND '.join(where)

        if select_all:
            sql = '''SELECT *
                     FROM files
                     {0}
                  '''.format(where)
        else:
            sql = '''SELECT location, name
                     FROM files
                     {0}
                  '''.format(where)


        with self.create_connection() as conn:
            cur = conn.cursor()

            if max_n_files != -1:
                sql += 'LIMIT ?'
                feed_list += max_n_files,
            cur.execute(sql, feed_list)

            rows = cur.fetchall()

        if select_all:
            return rows
        else:
            return [x[0] + x[1] for x in rows]

    def erase_entry(self, _id):
        '''
        Erase an entry, requires knowing it's ID
        '''
        sql = 'DELETE FROM files WHERE id=?'
        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, (_id,))
        return

    def erase_stage(self, dataset, stage):
        '''
        Erase an entire stage's worth of entries
        '''

        if dataset is None:
            raise Exception('Must specify dataset')

        sql = 'DELETE FROM files WHERE dataset=? AND stage=?'
        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, (dataset, stage,))
        return

    def erase_dataset(self, dataset):
        '''
        Erase an entire data set from the database
        '''

        if dataset is None:
            raise Exception('Must specify dataset')

        sql = 'DELETE FROM files WHERE dataset=?'
        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, (dataset,))
        return

    def yield_files(self, dataset, stage, ftype, max_n_files=-1):
        '''Yield a list of files for consumption

        Finds files that match the description and marks them as
        in-progress consumption.  Unless files are marked as fully
        consumed, they will not be yielded again

        Arguments:
            stage {str} -- stage name
            ftype {int} -- file type (ana, artroot, etc)

        Keyword Arguments:
            max_n_files {number} -- [description] (default: {-1})

        Returns:
            list of files for use
        '''

        if dataset is None:
            raise Exception('Must specify dataset')


        cur = self.create_connection().cursor()
        sql = """SELECT *
                 FROM files
                 WHERE dataset=? AND stage=? AND type=? AND status=0 AND consumed=0
              """
        feed_list=[dataset, stage, ftype]
        if max_n_files != -1:
            sql += "LIMIT ?"
            feed_list += max_n_files,
        cur.execute(sql, feed_list)

        rows = cur.fetchall()

        # Now, update the database to mark the returned rows as consumed
        sql = """UPDATE files
                 SET consumed=1
                 WHERE id=?
              """
        try:
            with self.create_connection() as conn:
                cur = conn.cursor()
                for row in rows:
                    cur.execute(sql, (row[0],) )
                    # self.dump_all_files()
        except Error as e:
            print e
            print("Could not update database to consume files")
            return None

        return rows


    def reset_failed_files(self, dataset, stage, ftype):
        '''Reset files marked for consumption to unconsumed.

        Resets the consumption status from in-progress to un-consumed
        for files that were previously attempted for consumption.

        Arguments:
            stage {str} -- stage name
            ftype {int} -- file type (ana, artroot, etc)
        '''

        cur = self.create_connection().cursor()
        sql = """SELECT *
                 FROM files
                 WHERE dataset=? AND stage=? AND type=? AND status=0 AND consumed=1
              """
        feed_list=[dataset, stage, ftype]

        cur.execute(sql, feed_list)

        rows = cur.fetchall()

        # Now, update the database to mark the returned rows as consumed
        sql = """UPDATE files
                 SET consumed=0
                 WHERE id=?
              """
        try:
            with self.create_connection() as conn:
                cur = conn.cursor()
                for row in rows:
                    cur.execute(sql, (row[0],) )
        except Error as e:
            print("Could not update database to reset consumed files")


    def consume_files(self, dataset, files, stage, ftype):
        '''Mark specified files from in-progress to finalized consumption

        [description]

        Arguments:
            stage {[type]} -- [description]
            ftype {[type]} -- [description]

        Keyword Arguments:
            max_n_files {number} -- [description] (default: {-1})

        Returns:
            [type] -- [description]
        '''

        # For each file, query the data base to change the consumption status:
        sql = """UPDATE files
                 SET consumed=2
                 WHERE dataset=? AND stage=? AND type=? AND status=0 AND consumed=1 AND name=?
              """

        try:
            with self.create_connection() as conn:
                cur = conn.cursor()
                for fname in files:
                    feed_list=[dataset, stage, ftype, fname]
                    cur.execute(sql, feed_list)
        except Error as e:
            print e
            print("Could not update database to consume files")



    def initialize_from(self, input_db, dataset, stage):
        '''Copy input files from one db to this one

        Query the other database to copy files into this one

        Arguments:
            input_db {[type]} -- [description]
            dataset {[type]} -- [description]
            stage {[type]} -- [description]
        '''

        # If this database has any files in it, raise an exception:
        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM files")

            rows = cur.fetchall()

            if len(rows) != 0:
                raise Exception("Must only initialize from empty databases")

        # First, we need to get the info from the other dataset
        input_rows = input_db.list_files(dataset, stage,
                                         ftype=0, status=0,
                                         select_all=True)

        # Next, get a list of all information expected in the right order:
        with self.create_connection() as conn:
            cur = conn.cursor()
            sql = '''PRAGMA table_info(files)'''
            cur.execute(sql)
            columns = [x[1] for x in cur.fetchall()]


        values = '(' + ', '.join((len(columns))*['?']) + ')'

        # Now, insert those files into this database
        with self.create_connection() as conn:
            cur = conn.cursor()
            sql = '''INSERT INTO files ({0})
                     VALUES {1}'''.format(",".join(columns), values)
            print sql
            cur.executemany(sql, input_rows)

