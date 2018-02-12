import sys, os

from MySQLdb import Error as Error

from connect_db import read_connection
from ReaderBase import ReaderBase

class ProjectReader(ReaderBase):
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

    def list_datasets(self):
        '''List all declared datasets

        '''

        dataset_list_sql = '''
            SELECT dataset
            FROM dataset_master_index
        '''

        with self.connect() as conn:
            conn.execute(dataset_list_sql)
            return conn.fetchall()

    def dataset_ids(self, datasets):
        '''Return a list of primary keys for the datasets specified

        Return the same type as input, in this sense:
            - single string input returns single number
            - list input returns list

        Arguments:
            parents {[type]} -- [description]
        '''

        id_query_sql = '''
            SELECT id
            FROM dataset_master_index
            WHERE dataset=(%s)
        '''



        with self.connect() as conn:
            if isinstance(datasets, (str)):
                try:
                    conn.execute(id_query_sql, (datasets,))
                except Error as e:
                    print e
                    return None
                try:
                    return conn.fetchone()[0]
                except:
                    return None
            else:
                ids = []
                print datasets
                for dataset in datasets:
                    try:
                        conn.execute(id_query_sql, (dataset,))
                    except Error as e:
                        print e
                        return None
                    ids.append(conn.fetchone()[0])
                return ids


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
            dataset_id = self.dataset_ids(dataset_name)

        # Have ids, find the entries in the dataset_master_consumption
        # table that list these ids as daughters

        parent_lookup_sql = '''
            SELECT input FROM dataset_master_consumption
            WHERE output=%s
        '''

        with self.connect() as conn:
            print dataset_id
            conn.execute(parent_lookup_sql, (dataset_id,))
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
            _id = self.dataset_ids(dataset_name)
        else:
            _id = dataset_id
        # Have _id, find the entries in the dataset_master_consumption
        # table that list these _id as daughters

        daughter_lookup_sql = '''
            SELECT output
            FROM dataset_master_consumption
            WHERE input=%s
        '''

        with self.connect() as conn:
            conn.execute(daughter_lookup_sql, (_id,))
            daughter_ids = conn.fetchall()

        if return_mode == 0:
            return daughter_ids
        else:
            # Need to look up the names for these parents
            parent_name_sql = '''
                SELECT dataset from dataset_master_index
                WHERE id=%s
            '''

            with self.connect() as conn:
                conn.execute(parent_name_sql, daughter_ids)

                return conn.fetchall()