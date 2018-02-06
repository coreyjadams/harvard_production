import sys, os

from MySQLdb import Error as Error

from connect_db import read_connection
from ReaderBase import ReaderBase

def ProjectReader(ReaderBase):
    '''Class to read project tables

    This class can read and compute with project information

    Arguments:
        object {[type]} -- [description]
    '''

    def __init__(self):
        super(ProjectReader, self).__init__()



    def connect(self):
        '''Connect to the database

        Return a readonly connection to the database
        '''
        return read_connection(self._password_file)


    def dataset_ids(self, datasets):
        '''Return a list of primary keys for the datasets specified

        Arguments:
            parents {[type]} -- [description]
        '''

        id_query_sql = '''SELECT id
                          FROM dataset_master_index
                          WHERE dataset=?'''

        with self.connect() as conn:
            try:
                conn.execute(id_query_sql, datasets)
            except Error as e:
                print e
                return None

            return conn.fetchall()

    def get_dataset_ids(self, dataset_names):
        '''Get the id of the dataset from it's name
        '''

        name_lookup_sql = '''
            SELECT id from dataset_master_index
            WHERE name=?
        '''

        with self.connect() as conn:
            conn.executemany(name_lookup_sql, dataset_name)
            try:
                return conn.fetchone()[0]
            except:
                print "Could not look up id of datasets {0}".format(dataset_name)



    def direct_parents(self, dataset_id=None, dataset_name=None):
        ''' Return the id of the direct parents of this dataset

        If dataset_id is not None, returns the ids of the parents
        If dataset_name is not None, returns the names of the parents

        If dataset_id and dataset_name are both None, or both not None,
        raise an exception

        Keyword Arguments:
            dataset_id {int}   -- ID of the dataset (default: {None})
            dataset_name {str} -- Name of the dataset (default: {None})

        '''

        if dataset_id is None and dataset_name is None:
            raise Exception("Can't get parentage of None values")

        if dataset_id is not None and dataset_name is not None:
            raise Exception("Return value unspecified, please use only dataset_id OR dataset_name")

        return_mode = 0
        if dataset_id is None and dataset_name is not None:
            return_mode = 1
            # Get the dataset id:
            ids = self.get_dataset_ids(dataset_name)

        # Have ids, find the entries in the dataset_master_consumption
        # table that list these ids as daughters

        parent_lookup_sql = '''
            SELECT input FROM dataset_master_consumption
            WHERE output=?
        '''

        with self.connect() as conn:
            conn.executemany(parent_lookup_sql, ids)
            parent_ids = conn.fetchall()

        if return_mode == 0:
            return parent_ids
        else:
            # Need to look up the names for these parents
            parent_name_sql = '''
                SELECT dataset from dataset_master_index
                WHERE id=?
            '''

            with self.connect() as conn:
                conn.executemany(parent_name_sql, parent_ids)

                return conn.fetchall()

    def direct_daughters(self, dataset_id=None, dataset_name=None):
        '''Return the direct daughters of this dataset

        If dataset_id is not None, returns the ids of the daughters
        If dataset_name is not None, returns the names of the daughters

        If dataset_id and dataset_name are both None, or both not None,
        raise an exception

        Keyword Arguments:
            dataset_id {int}   -- ID of the dataset (default: {None})
            dataset_name {str} -- Name of the dataset (default: {None})
        '''

        if dataset_id is None and dataset_name is None:
            raise Exception("Can't get parentage of None values")

        if dataset_id is not None and dataset_name is not None:
            raise Exception("Return value unspecified, please use only dataset_id OR dataset_name")

        return_mode = 0
        if dataset_id is None and dataset_name is not None:
            return_mode = 1
            # Get the dataset id:
            ids = self.get_dataset_ids(dataset_name)

        # Have ids, find the entries in the dataset_master_consumption
        # table that list these ids as daughters

        daughter_lookup_sql = '''
            SELECT output FROM dataset_master_consumption
            WHERE input=?
        '''

        with self.connect() as conn:
            conn.executemany(daughter_lookup_sql, ids)
            daughter_ids = conn.fetchall()

        if return_mode == 0:
            return daughter_ids
        else:
            # Need to look up the names for these parents
            parent_name_sql = '''
                SELECT dataset from dataset_master_index
                WHERE id=?
            '''

            with self.connect() as conn:
                conn.executemany(parent_name_sql, daughter_ids)

                return conn.fetchall()