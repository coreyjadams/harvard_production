import sys, os

from MySQLdb import Error as Error


def DatasetReader(ReaderBase):
    '''Class to read project tables

    This class can read and compute with project information

    Arguments:
        object {[type]} -- [description]
    '''

    def __init__(self):
        super(DatasetReader, self).__init__()
        pass


    def connect(self):
        '''Connect to the database

        Return a readonly connection to the database
        '''
        return read_connection(self._password_file)


