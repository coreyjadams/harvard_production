import os

from config import ProjectConfig
from database import DBUtil

class ProjectHandler(object):
    '''
    This class takes the input from the command line, parses,
    and takes the action needed.
    '''
    def __init__(self, config_file, action, stage=None):
        super(ProjectHandler, self).__init__()
        self.config_file = config_file
        self.stage = stage
        self.action = action

        self.stage_actions = ['submit', 'clean', 'status', 'check']
        self.project_actions = ['check', 'clean']

        if stage is None and self.action not in self.project_actions:
            raise Exception("Action {} not available".format(self.action))
        elif stage is not None and self.action not in self.stage_actions:
            raise Exception("Action {} not available".format(self.action))

        # Build the configuration class:
        self.config = ProjectConfig(config_file)

        # Create the work directory:
        self.work_dir = self.config['top_dir'] + '/work/'
        self.make_directory(self.work_dir)

        # Create the project database as well:
        db_name =  self.work_dir + self.config['name'] + '.db'
        self.project_db = DBUtil(db_name)


    def act(self):
        if self.action == 'submit':
            self.submit()
        elif self.action == 'clean':
            self.clean()
        elif self.action == 'status':
            self.status()
        elif self.action == 'check':
            self.check()
        elif self.action == 'makeup':
            self.makeup()
        else:
            return



    def submit(self):
        '''
        Build a submission script, then call it to launch
        batch jobs.

        Slurm copies environment variables from the process that launches jobs,
        so we will make a child of the launching process in python and launch jobs
        with larsoft env variables set up.
        '''

        # Get the active stage:
        stage = self.config.stage(self.stage)

        # First part of 'submit' is to make sure the input, work
        # and output directories exist
        self.make_directory(stage.output_directory())
        self.make_directory(self.work_dir+str(stage.name))


        # Next, build a submission script to actually submit the jobs
        job_name = self.config['name'] + '.' + stage.name
        script_name = '{}_submission_script.slurm'.format(job_name)
        with open(script_name, 'w') as script:
            script.write('#!/bin/bash\n')
            script.write('#SBATCH --job-name={}\n'.format(job_name))
            script.write('#SBATCH --ntasks=1\n')
            script.write('#SBATCH --mem={}mb\n'.format(stage['memory']))
            script.write('#SBATCH --time={}\n'.format(stage['time']))
            script.write('#SBATCH --output=array_%A-%a.log\n')
            # script.write('#SBATCH --array=0-{}                   # Array range'.format(stage.n_jobs()))
            script.write('\n')
            script.write('#Below is the python script that runs on each node:\n')
            script.write('python run_job.py {} {} {}\n'.format(self.config_file, self.stage, self.project_db.file()))


        print 'Submitting jobs ... [not really]'
        # Here is the command to actually submit jobs:
        command = ['sbatch', '-a', '0-{}'.format(stage.n_jobs()-1), script_name]
        print command

    def make_directory(self, path):
        '''
        Make a directory safely
        '''
        try:
            os.makedirs(path)
        except OSError:
            if not os.path.isdir(path):
                raise

    def clean(self):
        '''
        Clean the project.  If stage is none clean the whole thing.
        Otherwise, clean only that stage.  If cleaning everything, clean the database file
        Only when files are deleted
        '''

        if not self.get_clean_confirmation():
            return
        # If stage is set, clean that stage only:
        if stage is not None:
            # Remove files from the database and purge them from disk:
            for f in self.project_db.dump_all_files():
                os.remove(f)
            os.path.removedir(stage.output_directory())
            os.path.removedir(self.work_dir+str(stage.name))
        pass


    def get_clean_confirmation(self):
        '''
        Force the user to confirm he/she wants to clean things up
        '''
        print 'You are requesting to clean the following stages:'
        if self.stage is not None:
            print '  {}'.format(self.stage)
        else:
            for stage in self.project.stages():
                print '  {}'.format(stage.name)
        confirmation = raw_input('Please confirm this is the intended action (type \"y\"): ')
        if confirmation.lower() in ['y', 'yes']:
            return True
        return False

    def status(self):
        '''
        The status function reads in the job id number from the work directory
        and queries the scheduler to get job status.
        '''

        pass

    def check(self):
        '''
        The check function parses the data base and prints out information
        about number of completed files and number of events processed
        '''
        pass

    def makeup(self):
        '''Run makeup jobs

        Search the list of completed jobs, and query how many jobs are not running

        If no jobs are running, submit jobs to complete the previous stage of running.
        '''