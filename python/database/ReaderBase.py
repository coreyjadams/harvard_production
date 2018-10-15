import sys, os

from MySQLdb import Error as Error

from connect_db import read_connection

class ReaderBase(object):

    def __init__(self):
        self._password_file = "/n/home03/guenette/ProductionFile/mysqldb"

    def connect(self):
        return read_connection(self._password_file)
