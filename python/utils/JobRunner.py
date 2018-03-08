import os
import subprocess
import glob
import time
import shutil

from database import ProjectUtils, DatasetUtils

class cd:
    """Context manager for changing the current working directory

    Taken from: https://stackoverflow.com/questions/431684/how-do-i-cd-in-python/13197763#13197763
    """
    def __init__(self, newPath):
        self.newPath = os.path.expanduser(newPath)

    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)

class JobRunner(object):
    """
    Class for running a single larsoft job.  Can use multiple files
    at once and handle larsoft commands
    """
    def __init__(self, project, stage):
        super(JobRunner, self).__init__()
        self.project = project
        self.stage = stage

        self.output_file = None
        self.ana_file = None
        self.return_code = None
        self.out_dir = None
        self.n_events = 0

    def prepare_job(self):
        '''
        Prepare everything needed for running a job.
        '''

        # Prepare an area on the scratch directory for working:
        self.work_dir  = '/n/regal/guenette_lab/{0}/{1}/'.format(self.project['name'], self.stage.name)
        self.work_dir += os.environ['SLURM_ARRAY_JOB_ID'] + "."
        self.work_dir += os.environ['SLURM_ARRAY_TASK_ID'] + '/'
        try:
            os.makedirs(self.work_dir)
        except OSError:
            if not os.path.isdir(self.work_dir):
                raise



        # If necessary, make the output folder.
        self.out_dir = self.stage.output_directory() + "/"
        self.out_dir += os.environ['SLURM_ARRAY_JOB_ID'] + "."
        self.out_dir += os.environ['SLURM_ARRAY_TASK_ID'] + '/'
        try:
            os.makedirs(self.out_dir)
        except OSError:
            if not os.path.isdir(self.out_dir):
                raise


        # # Make sure failed files are reset:
        # db_util.reset_failed_files(dataset=self.stage.input_dataset(),
        #     stage=self.stage.name,
        #     ftype=0)




    def run_job(self, job_id, env=None):
        '''
        Run the actual larsoft job with subprocess
        Since each stage can take multiple fcl files, this captures the information
        from each individually.  It feeds the output of one into the input for the next
        '''
        pass
