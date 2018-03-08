from collections import OrderedDict

import yaml

from ConfigException import ConfigException
from LarsoftConfig import LarsoftConfig
from StageConfig import StageConfig

class ProjectConfigException(ConfigException):
    ''' Custom exception for the entire project'''

    def __init__(self, message = None, key = None):
        message = 'Error Configuring Project'
        if key is not None:
            message += ': Missing keyword {}'.format(key)
        super(ProjectConfigException, self).__init__(message)

class ProjectConfig(object):
    '''
    Larsoft Configuration Object
    Stores the information from the larsoft configuration and includes
    helpful functions
    '''
    def __init__(self, config_file):
        super(ProjectConfig, self).__init__()

        # Parse the configuration file:
        try:
            with open(config_file, 'r') as _f:
                yml_dict = yaml.load(_f)
        except yaml.YAMLError as exc:
            raise exc
        except:
            raise ProjectConfigException(
                "Could not open file {0}".format(config_file))

        required_keys=['name','top_dir','software','stages']
        # Check for presence of required keys:
        for key in required_keys:
            if key not in yml_dict:
                raise ProjectConfigException(key=key)

        self.yml_dict = yml_dict

        # Build a larsoft configuation object:
        if self.yml_dict['software']['type'] == 'larsoft'
            self.software_config = LarsoftConfig(self.yml_dict['larsoft'])
        elif self.yml_dict['software']['type'] == ''

        # Build a list of stages:
        self.stages = OrderedDict()
        prev_stage=None
        for stage in self.yml_dict['stages']:
            name = stage.keys()[0]
            self.stages[name] = StageConfig(stage[name], name, prev_stage)
            prev_stage = name

    def __getitem__(self, key):
        return self.yml_dict[key]

    def software(self):
        return self.software_config

    def stage(self, name):
        try:
            return self.stages[name]
        except:
            print "WARNING: stage {0} not found, returning none.".format(name)
            return None

