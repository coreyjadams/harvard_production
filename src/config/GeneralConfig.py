import subprocess
import tempfile
import os

from SoftwareConfig import SoftwareConfig, SoftwareConfigException

class GeneralConfig(SoftwareConfig):
    '''
    Larsoft Configuration Object
    Stores the information from the larsoft configuration and includes
    helpful functions
    '''

    def __init__(self, yml_dict):
        super(GeneralConfig, self).__init__()

        # Check required keys are present
        required_keys=['setup_scripts']
        for key in required_keys:
            if key not in yml_dict:
                raise SoftwareConfigException(key=key)

        # Make a persistant reference to the dictionary:
        self.yml_dict = yml_dict


    def setup(self, return_env=False):
        '''
        Function to set up larsoft.  Generates a temporary script, sets up
        everything in a temporary shell, copies the environment, and returns

        If return_env == True, this will return a copy of the environment
        in a dict.  Otherwise, it just sets environment variables directly.
        '''

        #############################################################
        # Create a temporary file and write the necessary commands,
        # then call the file in shell and save the environment
        #############################################################

        # Create a list of commands:
        shell_commands = ['#!/bin/bash']

        # If there is a local_products area (or multiple) set that up too:
        for setup_script in self.yml_dict['setup_scripts']:
            shell_commands.append("source {0}".format(setup_script))


        # Generate a small temporary script to set up environments
        fd, path = tempfile.mkstemp(prefix='setup', suffix='.sh')

        env_dict = dict()
        print('Setting up software with file {0}'.format(path))
        try:
            with os.fdopen(fd, 'w') as tmp:
                # do stuff with temp file
                for comm in shell_commands:
                    tmp.write(comm + '\n')
                    print('  ' + comm)
                tmp.write('\n')

            command = ['bash', '-c',
               'source {0} && echo \"<<<<<DO NOT REMOVE>>>>>\" && env\n'.format(path)]
            print(command)
            proc = subprocess.Popen(
                command, stdout = subprocess.PIPE,
                stderr = subprocess.PIPE)

            stdout, stderr = proc.communicate()

            # If the command was successfull, then source the script.
            # If not successfull, print out information and raise and exception
            if proc.returncode != 0:
                print "Error in software setup"
                print "Output:\n{0}".format(stdout)
                print "Error:\n{0}".format(stderr)
                raise SoftwareConfigException()
            else:
                found = False
                for line in stdout.split("\n"):
                    if '<<<<<DO NOT REMOVE>>>>>' in line:
                        found = True
                        continue
                    if not found:
                        continue
                    (key, _, value) = line.partition("=")
                    # print line.partition("=")
                    env_dict[key] = value
        finally:
            os.remove(path)

        if return_env:
            return env_dict
        else:
            for key, value in env_dict.iteritems():
                os.environ[key] = value
