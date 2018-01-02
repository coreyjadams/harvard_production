import os
import sqlite3

class StageDatabase(object):
    """
    This class maintains and provides access
    to a database file for a stage.  It provides some worker
    functions to make things easier.

    Database columns:
    
    ***Each entry in the database represents a file***

    
    Project  - the project name that produced that file (str)
    User     - the user that produced that file (str)

    Input    - Is this an input file (bool)
    Output   - Is this an output file (bool)


    """
    def __init__(self, stage):
        super(StageDatabase, self).__init__()
        
        


    def get_all_finished_files(self):
        '''
        Query the data base and return all of the produced files
        '''

    def get_next