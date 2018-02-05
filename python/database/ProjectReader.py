import sys, os

from connect_db import read_connection

def ProjectReader(object):
    '''Class to read project tables

    This class can read and compute with project information

    Arguments:
        object {[type]} -- [description]
    '''

    def __init__(self):
        self._password_file = "connection_info.yml"
        pass

    def connect(self):
        '''Connect to the database

        Return a readonly connection to the database
        '''
        return read_connection(self._password_file)