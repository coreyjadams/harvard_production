import os
import subprocess
import glob
import time
import shutil
import random

from database import ProjectUtils, DatasetUtils, DatasetReader

from JobRunner import JobRunner, cd

class ICRunner(JobRunner):
    """
    Class for running a single larsoft job.  Can use multiple files
    at once and handle larsoft commands
    """
    def __init__(self, project, stage):
        super(ICRunner, self).__init__(project, stage)
        self.project = project
        self.stage = stage

        self.output_file = None
        self.ana_file = None
        self.return_code = None
        self.out_dir = None
        self.n_events = 0



    def run_job(self, job_id, env=None):
        '''
        Run the actual job with subprocess
        Since each stage can take multiple fcl files, this captures the information
        from each individually.  It feeds the output of one into the input for the next
        '''

        dataset_util = DatasetUtils()

        print self.out_dir
        # To run the job, we move to the scratch directory:
        print("Changing directory to " + self.work_dir)
        with cd(self.work_dir):

            # Prepare the first input files, if there are any:
            if self.stage.has_input():
                raise Exception("The nexus runner does not expect input files.")

            inputs = None
            original_inputs = None

            # What is the config and init file?
            # Copy them locally, and rename based on job id:

            # Get the config:
            config = os.path.basename(self.stage.config())
            config = config.replace('template', job_id)
            shutil.copy(self.stage.config(), config)

            # Get the init:
            init = os.path.basename(self.stage['init'])
            init = init.replace('template', job_id)
            shutil.copy(self.stage['init'], init)

            # if there are extra files get them too:
            if 'extra_scripts' in self.stage.yml_dict:
                for extra_script in self.stage.yml_dict['extra_scripts']:
                    shutil.copy(extra_script, './')


            # Get a random seed and update the file name in the config and init script:
            random_seed = random.randint(0, 1e7)
            # Read the config, replace the values:
            with open(config,'r') as _cfg:
                config_text = _cfg.read()
                config_text = config_text.format(random_seed = random_seed,
                    file_index = job_id)
            with open(init, 'r') as _init:
                init_text = _cfg.read()
                init_text = init_text.format(file_index = job_id)

            # Write the files again:
            with open(config,'w') as _cfg:
                _cfg.write(config_text)
            with open(init, 'w') as _init:
                _init.write(init_text)

            # From here, we are ready to run.

            print("Running script: " + config)
            print("Using as inputs: " + str(inputs))
            return_code, n_events, output_file = self.run_script(init, env)
            if return_code != 0:
                # Copy all log files back:
                for _file in glob.glob('./*.log'):
                    shutil.copy(_file, self.out_dir)
                raise Exception("config file " + init + " failed to exit with status 0.")



            # Here, all the config files have run.  Save the final products:

            self.output_file = output_file
            self.return_code = return_code
            self.n_events = n_events



        # Copy the output files to the output directory
        src_files = os.listdir(self.work_dir)
        for file_name in src_files:
            full_file_name = os.path.join(self.work_dir, file_name)
            if (os.path.isfile(full_file_name)):
                shutil.copy(full_file_name, self.out_dir)

        if self.output_file is None and self.ana_file is None:
            raise Exception('ERROR: no output produced at all.')


        # Declare the output to the database
        output_size = os.path.getsize(self.out_dir + self.output_file)
        out_id = dataset_util.declare_file(dataset=self.stage.output_dataset(),
                                 filename="{0}/{1}".format(self.out_dir, self.output_file),
                                 ftype=0,
                                 nevents=self.n_events,
                                 jobid=job_id,
                                 size=output_size)


        # Clear out the work directory:
        shutil.rmtree(self.work_dir)

    def run_script(self, init, env=None):




        # Build up a nexus command to run
        command = [self.project.software()['executable']]

        # The init script is ready to run.

        # Fcl file:
        command += [str(init) ]

        # The output file is contained in the config script

        # Number of events to generate:
        if self.stage.events_per_job() is not None:

            command += ['-n', str(self.stage.events_per_job())]

        # Configure the environment:
        if env is None:
            env = dict(os.environ)

        # Write the command to a file for record keeping:
        with open(self.work_dir + '/{0}_command.txt'.format(os.path.basename(fcl)), 'w') as _out:
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

            return return_code, None, None


        # We want to know how many events are in the file.
        # We actually have to run the dstviewer process to find this out.
        # First, find the output file:
        output_file = glob.glob('*.next')

        if len(output_file) != 1:
            raise Exception("Could not identify the output file correctly.")
        else:
            output_file = output_file[0]


        # Run the dstviewer to get the number of events:
        command = ['dstviewer', '--run', output_file, '0']

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

        if return_code != 0:
            raise Exception('Could not run dstviewer on the output file.')

        # Parse this output (stdout):
        for line in stdout.split('\n'):
            if 'Number of events:' in line:
                tokens = line.split(':')
                n_events = int(tokens[-1])



        return (return_code, n_events, output_file)


