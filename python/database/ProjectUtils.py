import sys, os

from MySQLdb import Error as Error

from connect_db import admin_connection, write_connection

from ProjectReader import ProjectReader

class ProjectUtils(ProjectReader):
    '''Class to manage project

    Inherits project reader to use it's reading functions

    Arguments:
        object {[type]} -- [description]
    '''

    def __init__(self):
        super(ProjectUtils, self).__init__()
        pass

    def connect(self):
        return write_connection(self._password_file)

    def admin_connect(self):
        '''Connect to the database

        Return an admin connection to the database

        Overloads connect function from ProjectReader
        '''
        return admin_connection(self._password_file)

    def insert_dataset_to_index(self, dataset):
        # Try to create the entry in the master index table for this dataset
        dataset_insert_sql = '''
            INSERT INTO dataset_master_index(dataset)
            VALUES (%s);
        '''

        with self.connect() as conn:
            try:
                conn.execute(dataset_insert_sql, (dataset,))
            except Error as e:
                print e
                return False
            return conn.lastrowid

    def delete_dataset_from_index(self, dataset):
        # Try to create the entry in the master index table for this dataset
        dataset_delete_sql = '''
            DELETE FROM dataset_master_index
            WHERE dataset=%s;
        '''

        with self.connect() as conn:
            try:
                conn.execute(dataset_delete_sql, (dataset,))
            except Error as e:
                print e
                return False
        return True

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




        self.insert_dataset_to_index(dataset)
        primary_index = self.dataset_ids(dataset)

        # If there are parents, get their primary ids and add the entries
        # to the consumption table
        if parents is not None:
            parent_ids = self.dataset_ids(parents)
            if parent_ids is None:
                raise Exception("Couldn't get primary keys for specified parents")
            consupmtion_parentage_sql = '''
                INSERT INTO dataset_master_consumption(input,output)
                VALUES (%s,%s);
            '''



            with self.connect() as conn:
                for parent_id in parent_ids:
                    try:
                        conn.execute(consupmtion_parentage_sql, (parent_id, primary_index))
                    except Error as e:
                        print e
                        return False


        # At this point, the dataset has been added to the dataset_master_index
        # table, and if there are parents the dataset_master_consumption table has been
        # also updated.

        # Create the search, metadata, and consumption table for this dataset
        # (consumption only if needed)

        if not self.create_dataset_metadata_table(dataset):
            return False

        if parents is not None:
            if not self.create_dataset_consumption_table(dataset, parents):
                return False

        # We are finished here, return True
        return True



    def create_dataset_metadata_table(self, dataset):
        table_name = "{0}_metadata".format(dataset)
        metadata_table_creation_sql = """
            CREATE TABLE IF NOT EXISTS {name} (
                id       INTEGER       NOT NULL AUTO_INCREMENT,
                filename VARCHAR(500)  NOT NULL UNIQUE,
                type     INTEGER       NOT NULL,
                nevents  INTEGER       NOT NULL,
                created  TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
                jobid    VARCHAR(50)   NOT NULL,
                size     INTEGER       NOT NULL,
                PRIMARY KEY (id)
            ); """.format(name=table_name)

        with self.admin_connect() as conn:
            try:
                conn.execute(metadata_table_creation_sql)
            except Error as e:
                print e
                print "Could not create metadata table"
                return False
        return True

    def create_dataset_consumption_table(self, dataset, parents):
        table_name = "{0}_consumption".format(dataset)
        output_file_table = "{0}_metadata".format(dataset)
        search_table_creation_sql = """
            CREATE TABLE IF NOT EXISTS {name} (
                id           INTEGER  NOT NULL AUTO_INCREMENT,
                inputfile    INTEGER  NOT NULL,
                inputproject INTEGER  NOT NULL,
                outputfile   INTEGER,
                jobid        VARCHAR(25),
                consumption  INTEGER  NOT NULL DEFAULT 0,
                PRIMARY KEY (id)
            ); """.format(name=table_name)

        with self.admin_connect() as conn:
            try:
                conn.execute(search_table_creation_sql)
            except Error as e:
                print e
                print "Could not create consumption table"
                return False

        # The table has been created, but now it needs to be populated from the
        # input tables.

        for parent in parents:
            # Find the dataset ID for the parent:
            parent_id = self.dataset_ids(parent)

            # print "This ID:   " + str()

            # Find the file ids for the input files from this parent
            # Only take full output files
            table_name = "{0}_metadata".format(parent)
            selection_sql = """
                SELECT id FROM {name}
                WHERE (type=0)
            """.format(name=table_name)

            with self.connect() as conn:
                conn.execute(selection_sql)
                file_ids = conn.fetchall()

            # Prepare data for insertion:
            insertion_data = [ [file_id[0], parent_id ] for file_id in file_ids]
            table_name = "{0}_consumption".format(dataset)
            file_insertion_sql = '''
                INSERT INTO {name}(inputfile, inputproject)
                VALUES (%s,%s)
            '''.format(name=table_name)

            with self.connect() as conn:
                conn.executemany(file_insertion_sql, insertion_data)
        return True


    def drop_dataset(self, dataset):
        '''Drop a dataset from the database

        Removes the following tables from the database:
         - dataset_metadata
         - dataset_consumption (if exists)

        Additionally, if this dataset has parents:
         - the master_dataset_consumption table is updated
           to remove this dataset's consumption of parents


        Additionally, if this dataset has children:
         - the master_dataset_consumption table is updated to
           remove the consumption of the children by this dataset

        Arguments:
            dataset {[type]} -- [description]
        '''

        # First, need the id of the dataset:
        dataset_id = self.dataset_ids(dataset)
        print "Dataset id: " + str(dataset_id)

        if dataset_id is None:
            print "Can not drop dataset {0} as it does not have a dataset id.".format(dataset)
            return False

        # Next, determine if this dataset has parents.
        # if it has parents, we need to drop it's consumption table too.

        parents = self.direct_parents(dataset_id=dataset_id)
        print "Parents: " + str(parents)
        if len(parents) > 0:
            has_parents = True
        else:
            has_parents = False

        daughters = self.direct_daughters(dataset_id=dataset_id)
        print "Daughters: " + str(daughters)
        if len(daughters) > 0:
            has_daughters = True
        else:
            has_daughters = False


        if has_parents:
            # Delete all rows from the consumption table
            # that reference this dataset as output
            parent_deletion_sql = '''
                DELETE FROM dataset_master_consumption
                WHERE output=%s
            '''
            with self.connect() as conn:
                conn.execute(parent_deletion_sql, (dataset_id,))

            # Also delete the consumption table for this dataset

        if has_daughters:
            # Delete all rows from the consumption table that reference
            # this dataset as input
            daughter_deletion_sql = '''
                DELETE FROM dataset_master_consumption
                WHERE input=%s
            '''
            with self.connect() as conn:
                conn.execute(daughter_deletion_sql, (dataset_id,))
            pass

        # Delete the tables for this project:
        with self.admin_connect() as conn:


            table_name = "{0}_metadata".format(dataset)
            drop_table_sql = '''DROP TABLE {table};'''.format(table=table_name)
            try:
                conn.execute(drop_table_sql)
            except Error as e:
                print e
                return False

            table_name = "{0}_consumption".format(dataset)
            drop_table_sql = '''DROP TABLE IF EXISTS {table};'''.format(table=table_name)
            try:
                conn.execute(drop_table_sql)
            except Error as e:
                print e
                return False


            # Remove this entry from the index:
            self.delete_dataset_from_index(dataset)

            pass