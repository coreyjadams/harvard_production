import os
import subprocess
import glob
import time


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
        self.stdout = None
        self.stderr = None
        self.n_events = 0

    def prepare_job(self):
        '''
        Prepare everything needed for running a job.
        '''

        # If necessary, make the output folder.
        self.out_dir = self.stage.output_directory() + "/"
        self.out_dir += os.environ['SLURM_ARRAY_JOB_ID'] + "."
        self.out_dir += os.environ['SLURM_ARRAY_TASK_ID'] + '/'
        try:
            os.makedirs(self.out_dir)
        except OSError:
            if not os.path.isdir(self.out_dir):
                raise




    def run_job(self, env=None):
        '''
        Run the actual larsoft job with subprocess
        '''

        print self.out_dir

        # Build up a larsoft command to run
        command = ['lar']
        # Append all of the individual options:

        # Fcl file:
        command += ['-c', str(self.stage.fcl()) ]

        # Input file[s]
        # Check that this stage actually gets input:
        if self.stage.has_input():
            command.append('-s')
            for _file in self.stage.get_next_files(stage.name, self.stage.n_files()):
                command.append(_file)


        # Number of events to generate:
        command += ['-n', str(self.stage.events_per_job())]

        # Configure the environment:
        if env is None:
            env = dict(os.environ)

        # Write the command to a file for record keeping:
        with open(self.out_dir + '/larcommand.txt', 'w') as _out:
            _out.write(' '.join(command))

        # Actually run the command:
        proc = subprocess.Popen(command,
                                cwd=self.out_dir,
                                stdout = subprocess.PIPE,
                                stderr = subprocess.PIPE,
                                env=env)

        retval=proc.poll()
        # the loop executes to wait till the command finish running
        self.stdout=''
        self.stderr=''
        while retval is None:
            time.sleep(1.0)
            # while waiting, fetch stdout (including STDERR) to avoid crogging the pipe
            for line in iter(proc.stdout.readline, b''):
                self.stdout += line
            for line in iter(proc.stderr.readline, b''):
                self.stderr += line
            # update the return value
            retval = proc.poll()


        self.return_code = proc.returncode


    def check_job(self):
        '''
        This function parses the output of the larsoft job and determines
        necessary information
        '''

        # Write the stdout and std error to a file:
        with open(self.out_dir + '/larjob_standard_output.log', 'w') as _out:
            _out.write(self.stdout)
        with open(self.out_dir + '/larjob_standard_error.log', 'w') as _out:
            _out.write(self.stderr)

        # Write the return code to to a file too:
        with open(self.out_dir + '/larjob_returncode', 'w') as _out:
            _out.write(str(self.return_code))

        if self.return_code != 0:

            return False


        # Identify the outcome
        # Glob the output directory for .root files and the ones
        # that have 'hist' are the ana files
        root_files = glob.glob(self.out_dir + '/*.root')
        for _file in root_files:
            if 'hist'in _file:
                self.ana_file = _file
            else:
                self.output_file = _file

        # We want to know how many events are in the file.
        # We can get it by parsing the stdout stream from the job:
        for line in reversed(self.stdout.split('\n')):
            if 'TrigReport Events total = ' in line:
                # This is the line reporting the number of events
                # Split the line on the spaces and take the 5th element
                self.n_events = int(line.split(' ')[4])
                break




        return True

    def declare_output(self, db_util):
        '''
        declare the known output files to the database:
        '''
        db_util.declare_file(filename=self.output_file,
                             location=self.out_dir,
                             stage=self.stage.name,
                             status=0,
                             nevents=self.n_events,
                             ftype=0)
        db_util.declare_file(filename=self.ana_file,
                             location=self.out_dir,
                             stage=self.stage.name,
                             status=0,
                             nevents=self.n_events,
                             ftype=1)


