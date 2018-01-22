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


    def declare_file(self, filename, location, stage, status, nevents, ftype):
        '''
        Declare a file to the database

        '''

        with  self.create_connection() as conn:

            sql = '''INSERT INTO files(name,location,stage,status,nevents,type)
                     VALUES(?,?,?,?,?,?) '''
            cur = conn.cursor()
            f = (filename, location, stage, status, nevents, ftype)
            cur.execute(sql, f)


    def count_events(self, stage, ftype, status):
        '''Count number of declared events as specified


        Arguments:
            stage {[type]} -- [description]
        '''
        if stage is None and ftype is None and status is None:
            raise Exception('Can not query database without selection')

        where=[]
        feed_list=[]
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

        cur = self.create_connection().cursor()
        sql = '''SELECT SUM(id)
                 FROM files
                 {0}
              '''.format(where)

        cur.execute(sql, feed_list)
        results = cur.fetchone()[0]

        return results


    def count_files(self, stage, ftype, status):
        '''Count the number of files matching the description

        '''
        if stage is None and ftype is None and status is None:
            raise Exception('Can not query database without selection')

        where=[]
        feed_list=[]
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

        cur = self.create_connection().cursor()
        sql = '''SELECT COUNT(nevents)
                 FROM files
                 {0}
              '''.format(where)

        cur.execute(sql, feed_list)
        results = cur.fetchone()[0]

        return results




    def list_files(self, stage, ftype, status, max_n_files=-1):
        """
        Query all rows in the tasks table with parameters
        """

        # Build up a where query:
        if stage is None and ftype is None and status is None:
            raise Exception('Can not query database without selection')
        where=[]
        feed_list=[]
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

        cur = self.create_connection().cursor()
        sql = '''SELECT *
                 FROM files
                 {0}
              '''.format(where)

        if max_n_files != -1:
            sql += 'LIMIT ?'
            feed_list += max_n_files,
        cur.execute(sql, feed_list)

        rows = cur.fetchall()

        return rows

    def erase_entry(self, _id):
        '''
        Erase an entry, requires knowing it's ID
        '''
        sql = 'DELETE FROM files WHERE id=?'
        with self.create_connection as conn:
            cur = conn.cursor()
            cur.execute(sql, (_id,))
        return

    def erase_stage(self, stage):
        '''
        Erase an entire stage's worth of entries
        '''
        sql = 'DELETE FROM files WHERE stage=?'
        with self.create_connection as conn:
            cur = conn.cursor()
            cur.execute(sql, (stage,))
        return

    def yield_files(self,stage, ftype, max_n_files=-1):
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

        cur = self.create_connection().cursor()
        sql = """SELECT *
                 FROM files
                 WHERE stage=? AND type=? AND status=0 AND consumed=0
              """
        feed_list=[stage, ftype]
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
        except Error as e:
            print("Could not update database to consume files")
            return None

        return rows


    def reset_failed_files(self, stage, ftype):
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
                 WHERE stage=? AND type=? AND status=0 AND consumed=1
              """
        feed_list=[stage, ftype]

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


    def consume_files(self, files, stage, ftype):
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
                 WHERE stage=? AND type=? AND status=0 AND consumed=1 AND name=?
              """
        try:
            with self.create_connection() as conn:
                cur = conn.cursor()
                for fname in files:
                    feed_list=[stage, ftype, fname]
                    cur.execute(sql, feed_list)
        except Error as e:
            print("Could not update database to consume files")

