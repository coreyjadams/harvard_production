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
    Consumed - True or false, if the file is consumed by the next stage
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


    def consume_files(self, stage, ftype, max_n_files=-1):
        """
        Get a list of files, and update the selected files to mark consumed as True
        """

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
            print "Could not update database to consume files"
            return None

        print rows
