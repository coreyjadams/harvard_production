import sys, os

from MySQLdb import Error as Error

from connect_db import write_connection


def DatasetUtils(DatasetReader):
    '''Class to manage project

    Inherits project reader to use it's reading functions

    Arguments:
        object {[type]} -- [description]
    '''

    def __init__(self):
        super(DatasetUtils, self).__init__()
        pass

    def connect(self):
        '''Connect to the database

        Return an admin connection to the database

        Overloads connect function from ProjectReader
        '''
        return write_connection(self._password_file)


    def reset_consumption_table(self, dataset):
        '''Reset the consumption table for this dataset

        This unfunction *only* updates files that are not fully consumed
        to be unconsumed

        Arguments:
            dataset {[type]} -- [description]
        '''
        table_name = "{0}_consumption".format(dataset)


        sql = """SELECT id
                 FROM {table}
                 WHERE consumed=1
              """.format(table=table_name)

        with self.connect() as conn:

            conn.execute(sql)

            ids = cur.fetchall()

        # Now, update the database to mark the returned rows as consumed
        sql = """UPDATE {table}
                 SET consumed=0
                 WHERE id=?
              """.format(table_name)
        with self.connect() as conn:
            cur.executemany(sql, ids )
