import os
import subprocess
import glob
import time
import shutil

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
        self.work_dir  = '/scratch/{0}/{1}/'.format(self.project['name'], self.stage.name)
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





    def run_job(self, db_util, env=None):
        '''
        Run the actual larsoft job with subprocess
        Since each stage can take multiple fcl files, this captures the information
        from each individually.  It feeds the output of one into the input for the next
        '''

        print self.out_dir
        # To run the job, we move to the scratch directory:
        print("Changing directory to " + self.work_dir)
        with cd(self.work_dir):

            # Prepare the first input files, if there are any:
            if self.stage.has_input():
                inputs = self.stage.get_next_files(stage.name, self.stage.n_files())
            else:
                inputs = None

            for fcl in self.stage.fcl():
                print("Running fcl: " + fcl)
                print("Using as inputs: " + inputs)
                return_code, n_events, output_file, ana_file = self.run_fcl(fcl, inputs, env)
                # set the output as the next input:
                inputs = [output_file]


            # Here, all the fcl files have run.  Save the final products:

            self.output_file = output_file
            self.ana_file = ana_file
            self.return_code = return_code
            self.n_events = n_events

            # Remove the temporary root files that will crogg up disk space
            root_files = [os.path.basename(x) for x in glob.glob(self.work_dir + '/*.root')]
            for file_name in root_files:
                if file_name == self.output_file:
                    continue
                if self.stage['ana_name'] in file_name:
                    continue
                os.remove(file_name)

        # Copy the output files to the output directory
        src_files = os.listdir(self.work_dir)
        for file_name in src_files:
            full_file_name = os.path.join(self.work_dir, file_name)
            if (os.path.isfile(full_file_name)):
                shutil.copy(full_file_name, self.out_dir)


        # Declare the output to the database
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


    def run_fcl(self, fcl, input_files, env=None):
        '''Run a fcl file as part of a job

        This function takes inventory of the current root files, runs the
        specified fcl file, spots new files and tags as ana/output, and returns
        the output file and the number of events processed.



        Arguments:
            fcl {str} -- fcl file to run
            input_files {list or None} -- list of files to use as input, or None for no input

        Keyword Arguments:
            env {dict or None} -- over ride the environment (default: {None})

        Returns:
            bool {tuple} -- (number of events processed, output files or None)
        '''

        # Take survey of the root files before the job starts:
        initial_root_files = [os.path.basename(x) for x in glob.glob(self.work_dir + '/*.root')]

        # Build up a larsoft command to run
        command = ['lar']

        # Fcl file:
        command += ['-c', str(fcl) ]

        # Input file[s]
        # Check that this stage actually gets input:
        if input_files is not None:
            command.append('-s')
            for _file in input_files:
                command.append(_file)


        # Number of events to generate:
        command += ['-n', str(self.stage.events_per_job())]

        # Configure the environment:
        if env is None:
            env = dict(os.environ)

        # Write the command to a file for record keeping:
        with open(self.work_dir + '/{0}_larcommand.txt'.format(os.path.basename(fcl)), 'w') as _out:
            _out.write(' '.join(command))


        # Actually run the command:
        proc = subprocess.Popen(command,
                                cwd = self.work_dir,
                                stdout = subprocess.PIPE,
                                stderr = subprocess.PIPE,
                                env=env)

        retval=proc.poll()
        # the loop executes to wait till the command finish running
        stdout=''
        stderr=''
        while retval is None:
            time.sleep(1.0)
            # while waiting, fetch stdout (including STDERR) to avoid crogging the pipe
            for line in iter(proc.stdout.readline, b''):
                stdout += line
            for line in iter(proc.stderr.readline, b''):
                stderr += line
            # update the return value
            retval = proc.poll()


        return_code = proc.returncode

        # Write the output to file:
        with open(self.work_dir + '/{0}_standard_output.log'.format(os.path.basename(fcl)), 'w') as _out:
            _out.write(stdout)
        with open(self.work_dir + '/{0}_standard_error.log'.format(os.path.basename(fcl)), 'w') as _out:
            _out.write(stderr)

        # Write the return code to to a file too:
        with open(self.work_dir + '/{0}_returncode'.format(os.path.basename(fcl)), 'w') as _out:
            _out.write(str(return_code))

        if return_code != 0:

            return 0, None, None, None


        # We want to know how many events are in the file.
        # We can get it by parsing the stdout stream from the job:
        foundOutput = False
        foundNEvents = False
        n_events = 0
        output_file = None
        for line in reversed(stdout.split('\n')):
            if 'TrigReport Events total = ' in line:
                # This is the line reporting the number of events
                # Split the line on the spaces and take the 5th element
                n_events = int(line.split(' ')[4])
                foundNEvents = True
            if 'Closed output file' in line:
                # This line has the name of the output file, and a lot
                # of other garbage.  Have to sort this out:
                tokens = line.split('Closed output file')
                output_file = tokens[-1].rstrip('\n').replace('\"', '').replace(' ', '')
                foundOutput = True


            if foundOutput and foundNEvents:
                break



        if not foundOutput:
            raise Expection("Can't identify the output file.")


        # Identify the outcome
        # Glob the output directory for .root files and the ones
        # that have 'hist' are the ana files
        root_files = [os.path.basename(x) for x in glob.glob(self.work_dir + '/*.root')]
        ana_file = None
        for _file in root_files:
            if self.stage['ana_name'] in _file and _file not in initial_root_files:
                ana_file = _file

        return (return_code, n_events, output_file, ana_file)


