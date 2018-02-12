import sys, os

from MySQLdb import Error as Error

from connect_db import write_connection

from DatasetReader import DatasetReader

class DatasetUtils(DatasetReader):
    '''Class to manage project

    Inherits project reader to use it's reading functions

    Arguments:
        object {[type]} -- [description]
    '''

    def __init__(self):
        super(DatasetUtils, self).__init__()
        pass

    def connect(self):
        '''Connect to the database

        Return an admin connection to the database

        Overloads connect function from ProjectReader
        '''
        return write_connection(self._password_file)


    def reset_consumption_table(self, dataset):
        '''Reset the consumption table for this dataset

        This function *only* updates files that are not fully consumed
        to be unconsumed

        Arguments:
            dataset {[type]} -- [description]
        '''
        table_name = "{0}_consumption".format(dataset)


        sql = """SELECT id
                 FROM {table}
                 WHERE consumption=1
              """.format(table=table_name)

        with self.connect() as conn:

            conn.execute(sql)

            ids = conn.fetchall()

        # Now, update the database to mark the returned rows as consumed
        sql = """UPDATE {table}
                 SET consumption=0, jobid=NULL
                 WHERE id=%s
              """.format(table=table_name)
        with self.connect() as conn:
            conn.executemany(sql, ids )


    def declare_file(self, dataset, filename, run,
                     ftype, nevents, jobid, size, parents=None):

        '''Declare a file to a dataset

        Adds this file to the dataset table.  Does not update the consumption table.
        Returns the id of the file just added for use in updating the consumption table.
        '''

        table_name = "{0}_metadata".format(dataset)
        file_addition_sql = '''
            INSERT INTO {name}(filename, run, type, nevents, jobid, size)
            VALUES(%s,%s,%s,%s,%s,%s)
        '''.format(name=table_name)
        values=(filename, run, ftype, nevents, jobid, size)


        with  self.connect() as conn:

            conn.execute(file_addition_sql, values)
            this_id = conn.lastrowid

        if parents is not None:
            # parents must be a tuple
            # of the format ((datset id, file id), ... )
            if len(parents) != 2:
                raise Exception("parents must be a tuple of the format ((datset id, file id), ... )")

            for (parent_dataset_id, parent_file_id) in parents:
                print parent_dataset_id, parent_file_id

        return this_id


    def delete_file(self, dataset, file_ids=None, file_names=None):
        '''Delete a file from the dataset table

        [description]

        Keyword Arguments:
            file_ids {[type]} -- [description] (default: {None})
            file_names {[type]} -- [description] (default: {None})
        '''
        table_name = "{0}_metadata".format(dataset)

        if file_ids is None and file_names is None:
            raise Exception("Can't get parentage of None values")

        if file_ids is not None and file_names is not None:
            raise Exception("Return value unspecified, please use only file_ids OR file_names")

        return_mode = 0
        if file_ids is None and file_names is not None:
            return_mode = 1
            # Get the dataset id:
            _ids = self.file_ids(file_names)
        else:
            _ids = file_ids

        delete_sql = '''
            DELETE FROM {name}
            WHERE id=%s
        '''.format(name=table_name)

        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.executemany(delete_sql, (_ids,))
        return


    def yield_files(self, dataset, n, jobid):
        '''Pull files from the consumption table

        Gathers files that meet specified criteria and returns
        a tuple of ( (dataset_id, file_id), ...) up to n files
        '''

        # Need to unpack the arguments:
        # where, feed_list = self.file_query(**kwargs)

        # To ensure we don't crogg the database, first update
        # to mark the files we will select with the jobid:
        table_name = "{0}_consumption".format(dataset)
        update_sql = '''
            UPDATE {table}
            SET consumption=1, jobid = %s
            WHERE consumption=0
            LIMIT %s;
        '''.format(table=table_name)

        with self.connect() as conn:
            update_list = (jobid, n)
            conn.execute(update_sql, update_list)

        # Now, select the files that have been marked for this job:

        select_sql = '''
            SELECT inputfile, inputproject
            FROM {table}
            WHERE jobid=%s AND consumption=1
        '''.format(table=table_name)

        with self.connect() as conn:
            select_list = (jobid,)
            conn.execute(select_sql, select_list)

        results = conn.fetchall()

        # Now, unpack the ids into file locations:
        project_lookup_sql = '''
            SELECT dataset
            FROM dataset_master_index
            WHERE id=%s
        '''
        file_lookup_sql = '''
            SELECT filename
            FROM   {table}
            WHERE  id=%s
        '''

        yielded_files = []
        for [fileid, projectid] in results:
            with self.connect() as conn:
                conn.execute(project_lookup_sql, (projectid,))
                name = conn.fetchone()[0]
                table_name = "{0}_metadata".format(name)
                conn.execute(file_lookup_sql.format(table=table_name), (fileid,) )
                yielded_files.append(conn.fetchone()[0])

        return yielded_files

    def consume_files(self, dataset, jobid, output_file_id):

        # Update the consumpution table for these files:
        table_name = "{0}_consumption".format(dataset)
        update_sql = '''
            UPDATE {table}
            SET consumption=2, outputfile=%s
            WHERE consumption=1 AND jobid=%s
        '''.format(table=table_name)

        with self.connect() as conn:
            conn.execute(update_sql, (output_file_id, jobid))
        return
