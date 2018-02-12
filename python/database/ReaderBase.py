import sys, os

from MySQLdb import Error as Error

from connect_db import read_connection

class ReaderBase(object):

    def __init__(self):
        self._password_file = "/home/cadams/Harvard-Production/production-tools/python/database/connection_info.yml"

    def connect(self):
        return read_connection(self._password_file)