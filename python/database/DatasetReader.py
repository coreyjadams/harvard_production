import sys, os

from MySQLdb import Error as Error

from ReaderBase import ReaderBase

class DatasetReader(ReaderBase):
    '''Class to read project tables

    This class can read and compute with project information

    Arguments:
        object {[type]} -- [description]
    '''

    def __init__(self):
        super(DatasetReader, self).__init__()
        pass

    def metadata_header(self, dataset):
        '''Return the header information for a dataset metadata table

        Arguments:
            dataset {[type]} -- [description]
        '''
        sql = '''
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME=(%s)
        '''
        with self.connect() as conn:
            try:
                conn.execute(sql, (dataset,))
            except Error as e:
                print e
                return None

            return conn.fetchall()

    def file_ids(self, dataset, filenames):
        '''Return a list of primary keys for the datasets specified

        Arguments:
            parents {[type]} -- [description]
        '''
        table_name = "{0}_metadata".format(dataset)
        id_query_sql = '''
            SELECT id
            FROM {table}
            WHERE filename=?
        '''.format(table=table_name)

        with self.connect() as conn:
            try:
                conn.execute(id_query_sql, filenames)
            except Error as e:
                print e
                return None

            return conn.fetchall()

    def file_query(self, **kwargs):
        where = []
        feed_list = []

        if not kwargs:
            return None, None

        for key, value in kwargs.iteritems():
            where.append(str(key) + " = %s")
            feed_list.append(value)

        return where, feed_list

    def select(self, dataset, select_string='*', limit=None, **kwargs):

        table_name = "{0}_metadata".format(dataset)
        where, feed_list = self.file_query(**kwargs)

        if where is not None:
            wherestring = ' AND '.join(where)
            select_sql = '''
                SELECT {select}
                FROM {table}
                WHERE {where}
            '''.format(select=select_string, table=table_name, where=wherestring)

        else:
            select_sql = '''
                SELECT {select}
                FROM {table}
            '''.format(select=select_string, table=table_name)

        if limit is not None and type(limit) == int:
            select_sql += "\n LIMIT {limit}".format(limit=limit)

        with self.connect() as conn:

            if feed_list is not None:
                conn.execute(select_sql, feed_list)
            else:
                conn.execute(select_sql)
            results = conn.fetchall()

        return results

    def count_files(self, dataset, **kwargs):

        table_name = "{0}_metadata".format(dataset)

        where, feed_list = self.file_query(**kwargs)

        if where is not None:
            wherestring = ' AND '.join(where)
            count_sql = '''
                SELECT COUNT(id)
                FROM {table}
                WHERE {where}
            '''.format(table=table_name, where=wherestring)

        else:
            count_sql = '''
                SELECT COUNT(id)
                FROM {table}
            '''.format(table=table_name)

        with self.connect() as conn:

            if feed_list is not None:
                conn.execute(count_sql, feed_list)
            else:
                conn.execute(count_sql)
            results = conn.fetchone()[0]

        return results


    def sum(self, dataset, target, **kwargs):

        table_name = "{0}_metadata".format(dataset)

        where, feed_list = self.file_query(**kwargs)

        if where is not None:
            wherestring = ' AND '.join(where)
            count_sql = '''
                SELECT SUM({target})
                FROM {table}
                WHERE {where}
            '''.format(target=target, table=table_name, where=wherestring)

        else:
            count_sql = '''
                SELECT SUM({target})
                FROM {table}
            '''.format(target=target, table=table_name)

        with self.connect() as conn:

            if feed_list is not None:
                conn.execute(count_sql, feed_list)
            else:
                conn.execute(count_sql)
            results = conn.fetchone()[0]

        return results

    def list_file_locations(self, dataset):

        table_name = "{0}_metadata".format(dataset)
        file_location_sql = '''
            SELECT location from {table}
        '''.format(table=table_name)

        with self.connect() as conn:
            try:
                conn.execute(file_location_sql)
                return conn.fetchall()
            except:
                return []

    def count_consumption_files(self, dataset, state):
        '''Return the number of unyielded files for this dataset

        Counts the number of files in the consumption table with status
        equal to 0.  If there is no consumption table, returns None

        Arguments:
            dataset {str} -- dataset name
        '''

        table_name = "{0}_consumption".format(dataset)

        if state == "unyielded":
            consumption = 0
        if state == "yieleded":
            consumption = 1
        if state == "consumed":
            consumption = 2

        unyielded_sql = '''
            SELECT COUNT(id)
            FROM {table}
            WHERE consumption={consumption}
        '''.format(table=table_name, consumption=consumption)


        with self.connect() as conn:
            try:
                conn.execute(unyielded_sql)
                return conn.fetchone()[0]
            except Exception as e:
                return None
