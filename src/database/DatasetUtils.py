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
        table_name = '{0}_consumption'.format(dataset)


        sql = '''SELECT id
                 FROM {table}
                 WHERE consumption=1
              '''.format(table=table_name)

        with self.connect() as conn:

            conn.execute(sql)

            ids = conn.fetchall()

        # Now, update the database to mark the returned rows as consumed
        sql = '''UPDATE {table}
                 SET consumption=0, jobid=NULL
                 WHERE id=%s
              '''.format(table=table_name)
        with self.connect() as conn:
            conn.executemany(sql, ids )


    def declare_file(self, dataset, filename,
                     ftype, nevents, jobid, size):

        '''Declare a file to a dataset

        Adds this file to the dataset table.  Does not update the consumption table.
        Returns the id of the file just added for use in updating the consumption table.
        '''

        table_name = '{0}_metadata'.format(dataset)
        file_addition_sql = '''
            INSERT INTO {name}(filename, type, nevents, jobid, size)
            VALUES(%s,%s,%s,%s,%s)
        '''.format(name=table_name)
        values=(filename, ftype, nevents, jobid, size)


        with  self.connect() as conn:

            conn.execute(file_addition_sql, values)
            this_id = conn.lastrowid

        return this_id


    def delete_file(self, dataset, file_ids=None, file_names=None):
        '''Delete a file from the dataset table

        [description]

        Keyword Arguments:
            file_ids {[type]} -- [description] (default: {None})
            file_names {[type]} -- [description] (default: {None})
        '''
        table_name = '{0}_metadata'.format(dataset)

        if file_ids is None and file_names is None:
            raise Exception('Can\'t get parentage of None values')

        if file_ids is not None and file_names is not None:
            raise Exception('Return value unspecified, please use only file_ids OR file_names')

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
        table_name = '{0}_consumption'.format(dataset)
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
                table_name = '{0}_metadata'.format(name)
                conn.execute(file_lookup_sql.format(table=table_name), (fileid,) )
                yielded_files.append(conn.fetchone()[0])

        return yielded_files

    def consume_files(self, dataset, jobid, output_file_id):

        # Update the consumpution table for these files:
        table_name = '{0}_consumption'.format(dataset)
        update_sql = '''
            UPDATE {table}
            SET consumption=2, outputfile=%s
            WHERE consumption=1 AND jobid=%s
        '''.format(table=table_name)

        with self.connect() as conn:
            conn.execute(update_sql, (output_file_id, jobid))
        return

    def add_job_id(self, dataset, jobid, n_jobs, workdir):

        '''Declare a jobid to a dataset campaign table

        Adds this job id info to a dataset campaign table.
        All jobs are automatically in the 'unknown' category
        '''

        if not isinstance(jobid, int):
            raise Exception('Can not create job id that is not an integer')




        table_name = '{0}_campaign'.format(dataset)
        jobid_addition_sql = '''
            INSERT INTO {name}(workdir, primary_id, n_jobs, n_success, n_failed, n_running, n_unknown)
            VALUES(%s,%s,%s,%s,%s)
        '''.format(name=table_name)
        values=(workdir, jobid, n_jobs, 0, 0, 0, n_jobs)


        with  self.connect() as conn:

            conn.execute(jobid_addition_sql, values)
            this_id = conn.lastrowid

        return this_id

    def update_job_array(self, dataset, jobarray, **kwargs):
        '''Update the job array information

        This function can ONLY update the following fields:
        n_failed
        n_running
        n_unknown

        They can all be passed as keyword arguments.  Any other keyword
        arguments will cause an exception.

        If it is not provided, n_unknown will be calculated from n_failed, n_running,
        and n_success.

        Arguments:
            dataset {str} -- dataset name
            jobarray {int} -- jobarray id
            **kwargs {dict} -- Fields to update
        '''

        for arg in kwargs:
            if arg not in ['n_failed', 'n_running', 'n_unknown']:
                raise Exception('Kwarg {0} not recognized for update_job_array'.format(arg))



        # Validate the input:
        if not isinstance(jobarray, int):
            raise Exception('Can not create job id that is not an integer')

        table_name = '{0}_campaign'.format(dataset)

        # Find out how many jobs this jobarray should have:
        n_job_sql = '''
            SELECT n_jobs, n_success
            FROM {table}
            WHERE primary_id=%s
        '''.format(table_name)

        with self.connect() as conn:
            array_list = (jobarray, )
            conn.execute(n_job_sql, array_list)
            n_jobs, n_success = conn.fetchall()[0]


        # Now we know n_jobs and n_completed, we can update n_failed, n_running, n_unknown.

        if 'n_running' not in kwargs:
            n_running = 0
        else:
            n_running = kwargs['n_running']
        if 'n_failed'  not in kwargs:
            n_failed = 0
        else:
            n_failed = kwargs['n_failed']
        if 'n_unknown' not in kwargs:
            n_unknown = n_jobs - n_success - n_running - n_failed
        else:
            n_unknown = kwargs['n_unknown']


        job_update_sql = '''
            UPDATE {table}
            SET n_failed=%s n_running=%s n_unknown=%s
        '''.format(table)

        with self.connect() as conn:
            update_list = (n_failed, n_running, n_unknown)
            conn.execute(job_update_sql, update_list)

        return


    def complete_job(self, dataset, jobarray):
        ''' Declare an individual job  successfully completed

        This does not modify a job array, just a particular entry in a job array
        '''


        if not isinstance(jobarray, int):
            raise Exception('Can not create job id that is not an integer')

        # Verify the jobarray is in the list of jobarrays:
        # TODO


        table_name = '{0}_campaign'.format(dataset)

        increment_completed_jobs_sql = '''
            UPDATE {table}
            SET n_success = n_success + 1
            WHERE primary_id = %s
        '''.format(table_name)

        with self.connect() as conn:
            array_list = (jobarray,)
            conn.execute(increment_completed_jobs_sql, array_list)

        return
