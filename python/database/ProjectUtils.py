import sys, os

from connect_db import admin_connection

def ProjectUtils(ProjectReader):
    '''Class to manage project

    Inherits project reader to use it's reading functions

    Arguments:
        object {[type]} -- [description]
    '''

    def __init__(self):
        self._password_file = "connection_info.yml"
        pass

    def connect(self):
        '''Connect to the database

        Return an admin connection to the database

        Overloads connect function from ProjectReader
        '''
        return admin_connection(self._password_file)


    def create_dataset(self, dataset, parents=None):
        '''Create a new dataset

        This function creates the tables for this dataset
        This creates the tables:
         - [dataset_name]_metadata
         - [dataset_name]_search

        If parents is not None, the table [dataset_name]_consumption is
        created and populated

        The main table dataset_master_index is updated to include this dataset

        If parents is None, the table dataset_master_consumption is updated

        Arguments:
            dataset {[type]} -- [description]
        '''

        # Create the search table for this dataset

        pass

