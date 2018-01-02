import os
import subprocess

class JobRunner(object):
    """
    Class for running a single larsoft job.  Can use multiple files
    at once and handle larsoft commands
    """
    def __init__(self, stage):
        super(JobRunner, self).__init__()
        self.stage = stage
            

    def run_job(self, env=None):
        '''
        Run the actual larsoft job with subprocess
        '''

        # Build up a larsoft command to run
        command = ['lar']
        # Append all of the individual options:

        # Fcl file:
        command += ['-c', str(self.stage.fcl()) ]

        # Input file[s]
        # Check that this stage actually gets input:
        if self.stage.has_input():
            command.append('-s')
            for _file in self.stage.get_next_files(self.stage.n_files()):
                command.append(_file)


        # Configure the environment:
        if env is None:
            env = dict(os.environ)

        print command

        # Actually run the command:
        proc = subprocess.Popen(command, 
                                stdout = subprocess.PIPE, 
                                stderr = subprocess.PIPE,
                                env=env)

        stdout, stderr = proc.communicate()
        return

