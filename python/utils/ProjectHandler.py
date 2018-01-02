import os

from config import ProjectConfig

class ProjectHandler(object):
    '''
    This class takes the input from the command line, parses,
    and takes the action needed.
    '''
    def __init__(self, config, stage, action):
        super(ProjectHandler, self).__init__()
        self.config = config
        self.stage = stage
        self.action = action

        self.available_actions = ['submit', 'clean', 'status', 'check']

        if self.action not in self.available_actions:
            print "Action {} not available".format(self.action)
            raise Exception

        # Build the configuration class:
        self.config = ProjectConfig(config)


    def act(self):
        if self.action == 'submit':
            self.submit()
        elif self.action == 'clean':
            self.clean()
        elif self.action == 'status':
            self.status()
        elif self.action == 'check':
            self.check()
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
        self.make_directory(stage.work_directory())

        # Next, build a 


        print 'Submitting jobs ... [not really]'

    def make_directory(self, path):
        '''
        Make a directory safely
        '''
        try: 
            os.makedirs(path)
        except OSError:
            if not os.path.isdir(path):
                raise