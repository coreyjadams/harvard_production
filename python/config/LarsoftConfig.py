import subprocess
import tempfile
import os

from ConfigException import ConfigException

class LarsoftConfigException(ConfigException):
    '''Specialized exception for configuration errors with larsoft block'''

    def __init__(self, message = None, key = None):
        message = 'Error Configuring Larsoft'
        if key is not None:
            message += ': Missing keyword {}'.format(key)
        super(LarsoftConfigException, self).__init__(message)

class LarsoftConfig(object):
    '''
    Larsoft Configuration Object
    Stores the information from the larsoft configuration and includes
    helpful functions
    '''

    def __init__(self, yml_dict):
        super(LarsoftConfig, self).__init__()

        # Check required keys are present
        required_keys=['product_areas','version','quals','product']
        for key in required_keys:
            if key not in yml_dict:
                raise LarsoftConfigException(key=key)

        # Make a persistant reference to the dictionary:
        self.yml_dict = yml_dict


    def setup_larsoft(self, return_env=False):
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
        # Add all of the product area set ups:
        for product_area in self.yml_dict['product_areas']:
            shell_commands.append("source {0}/setup ".format(product_area))
        # If there is a local_products area (or multiple) set that up too:
        if 'local_areas' in self.yml_dict:
            shell_commands.append('setup mrb')
            for local_area in self.yml_dict['local_areas']:
                shell_commands.append("source {0}/setup ".format(product_area))

        # Add the last command to actually setup the product
        shell_commands.append('setup {0} {1} -q {2}'.format(
            self.yml_dict['product'],
            self.yml_dict['version'],
            self.yml_dict['quals'],
            ))

        # Generate a small temporary script to set up environments
        fd, path = tempfile.mkstemp(prefix='larsetup', suffix='.sh')

        env_dict = dict()
        print('Setting up larsoft with file {0}'.format(path))
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
                print "Error in larsoft setup"
                print "Output:\n{0}".format(stdout)
                print "Error:\n{0}".format(stderr)
                raise LarsoftConfigException()
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
