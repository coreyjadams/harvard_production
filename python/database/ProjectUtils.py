import sys, os

from MySQLdb import Error as Error

from connect_db import admin_connection


def ProjectUtils(ProjectReader):
    '''Class to manage project

    Inherits project reader to use it's reading functions

    Arguments:
        object {[type]} -- [description]
    '''

    def __init__(self):
        super(ProjectUtils, self).__init__()
        pass

    def admin_connect(self):
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


        # Try to create the entry in the master index table for this dataset
        dataset_insert_sql = '''
            INSERT INTO dataset_master_index(
                dataset,
                hasparent,
                hasdaughter)
            VALUES (?,?,?);
        '''

        if parents is not None:
            values = (dataset, True,  False)
        else:
            values = (dataset, False, False)

        with self.connect() as conn:
            try:
                conn.execute(dataset_insert_sql, values)
            except Error as e:
                print e
                return False
            primary_id = conn.lastrowid

        # If there are parents, get their primary ids and add the entries
        # to the consumption table
        if parents is not None:
            parent_ids = self.dataset_ids(parents)
            if parent_ids is None:
                raise Exception("Couldn't get primary keys for specified parents")
            consupmtion_parentage_sql = '''
                INSERT INTO dataset_master_consumption(
                    input,
                    ouptut)
                VALUES (?,?);
            '''
            mark_parents_sql = '''
                UPDATE dataset_master_index
                SET daughters=1
                WHERE id=?
            '''


            input_vals  = [parent_ids, [primary_id]*len(parent_ids)]

            with self.connect() as conn:
                try:
                    conn.executemany(consupmtion_parentage_sql, input_vals)
                except Error as e:
                    print e
                    return False
                try:
                    conn.executeman(mark_parents_sql, parent_ids)
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
                id       INTEGER       PRIMARY KEY,
                filename VARCHAR(500)  NOT NULL UNIQUE,
                run      INTEGER       NOT NULL DEFAULT 0,
                type     INTEGER       NOT NULL,
                nevents  INTEGER       NOT NULL,
                created  TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
                jobid    BIGINT        NOT NULL,
                size     INTEGER       NOT NULL
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
                id           INTEGER  PRIMARY KEY,
                inputfile    INTEGER  NOT NULL,
                inputproject INTEGER  NOT NULL,
                outputfile   INTEGER,
                consumption  INTEGER  NOT NULL DEFAULT 0,
                FOREIGN KEY(inputproject) REFERENCES dataset_master_index(id) ON UPDATE CASCADE,
            ); """.format(name=table_name, reference=output_file_table)

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
            parend_id_sql = """
                SELECT id FROM dataset_master_index
                WHERE dataset=?
            """
            with self.connect() as conn:
                conn.execute(parend_id_sql)
                parent_id = int(conn.fetchone())

            # Find the file ids for the input files from this parent
            # Only take full output files
            table_name = "{0}_metadata".format(parent)
            selection_sql = """
                SELECT id FROM {name}
                WHERE (type=0)
            """

            with self.connect() as conn:
                conn.execute(selection_sql)
                file_ids = conn.fetchall()

            # Prepare data for insertion:
            insertion_data = ([parent_id]*len(file_ids), file_ids)
            file_insertion_sql = '''
                INSERT INTO {name}(inputfile, inputproject)
                VALUES=(?,?)
            '''.format(name=table_name)

            with self.connect() as conn:
                conn.execute(file_insertion_sql, insertion_data)
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

        with self.admin_connect() as conn:
            pass