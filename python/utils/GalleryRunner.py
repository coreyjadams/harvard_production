import os
import subprocess
import glob
import time
import shutil

from database import ProjectUtils, DatasetUtils

from JobRunner import JobRunner, cd

class GalleryRunner(JobRunner):
    """
    Class for running a single larsoft job.  Can use multiple files
    at once and handle larsoft commands
    """
    def __init__(self, project, stage):
        super(GalleryRunner, self).__init__(project, stage)
        self.project = project
        self.stage = stage

        self.output_file = None
        self.ana_file = None
        self.return_code = None
        self.out_dir = None
        self.n_events = 0



    def run_job(self, job_id, env=None):
        '''
        Run the actual larsoft job with subprocess
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
                print self.stage.n_files()
                inputs = dataset_util.yield_files(self.stage.output_dataset(),
                                                  self.stage.n_files(),
                                                  job_id)
                print inputs
                original_inputs = inputs
            else:
                inputs = None
                original_inputs = None

            # We keep track of output files produced on the node
            # Since we only keep the final output file,
            # outputs get deleted after the are used

            for fcl in self.stage.fcl():
                print("Running script: " + fcl)
                print("Using as inputs: " + str(inputs))
                return_code, n_events, output_file, ana_file = self.run_script(fcl, inputs, env)
                if return_code != 0:
                    # Copy all log files back:
                    for _file in glob.glob('./*.log'):
                        shutil.copy(_file, self.out_dir)
                    raise Exception("fcl file " + fcl + " failed to exit with status 0.")
                # set the output as the next input:
                if output_file is not None:
                    # Delete previous input files, as long as it's not
                    # the original input:
                    if inputs is not None:
                        for infile in inputs:
                            if original_inputs is None or infile not in original_inputs:
                                os.remove(infile)
                    inputs = [output_file]
                else:
                    inputs = None



            # Here, all the fcl files have run.  Save the final products:

            self.output_file = output_file
            self.ana_file = ana_file
            self.return_code = return_code
            self.n_events = n_events

            # Remove the temporary root files that will crogg up disk space
            root_files = [os.path.basename(x) for x in glob.glob(self.work_dir + '/*.root')]
            for file_name in root_files:
                if file_name == self.output_file or file_name == self.ana_file:
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
        if self.output_file is not None:
            output_size = os.path.getsize(self.out_dir + self.output_file)
            out_id = dataset_util.declare_file(dataset=self.stage.output_dataset(),
                                     filename="{0}/{1}".format(self.out_dir, self.output_file),
                                     ftype=0,
                                     nevents=self.n_events,
                                     jobid=job_id,
                                     size=output_size)


        ana_size = os.path.getsize(self.out_dir + self.ana_file)
        ana_id = dataset_util.declare_file(dataset=self.stage.output_dataset(),
                                 filename="{0}/{1}".format(self.out_dir, self.ana_file),
                                 nevents=self.n_events,
                                 ftype=1,
                                 jobid=job_id,
                                 size=ana_size)

        if self.stage['output']['anaonly']:
            out_id = ana_id

        # finalize the input:
        if original_inputs is not None:
            dataset_util.consume_files(self.stage.output_dataset(), job_id, out_id)

        # Clear out the work directory:
        shutil.rmtree(self.work_dir)

    def run_script(self, fcl, input_files, env=None):
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
        command = ['python']

        # Fcl file:
        command += [str(fcl) ]

        # Input file[s]
        # Check that this stage actually gets input:
        if input_files is not None:
            command.append('--files')
            for _file in input_files:
                command.append(_file)

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

            return return_code, None, None, None


        # We want to know how many events are in the file.
        # We can get it by parsing the stdout stream from the job:
        foundOutput = False
        foundNEvents = False
        n_events = 0
        output_file = None
        for line in reversed(stdout.split('\n')):
            if 'Number of entries processed:' in line:
                # This is the line reporting the number of events
                n_events = int(line.split(':')[1])
                foundNEvents = True
            if 'Output file name' in line:
                # This line has the name of the output file, and a lot
                # of other garbage.  Have to sort this out:
                tokens = line.split(':')
                print tokens
                output_file = tokens[-1].rstrip('\n').replace('\"', '').replace(' ', '')
                print output_file
                foundOutput = True


            if foundOutput and foundNEvents:
                break



        if not foundOutput:
            raise Exception("Can't identify the output file.")


        # Identify the outcome
        # Glob the output directory for .root files and the ones
        # that have 'hist' are the ana files
        root_files = [os.path.basename(x) for x in glob.glob(self.work_dir + '/*.root')]
        ana_file = output_file
        output_file = None

        return (return_code, n_events, output_file, ana_file)


