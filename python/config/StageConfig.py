
from ConfigException import ConfigException

class StageConfigException(ConfigException):
    ''' Custom exception for stages'''

    def __init__(self, key = None, name=None):
        message = 'Error Configuring Stage'
        if key is not None:
            message += ': Missing keyword {} in stage {}'.format(key, name)
        super(StageConfigException, self).__init__(message)

class StageConfig(object):
    '''
    Larsoft Configuration Object
    Stores the information from the larsoft configuration and includes
    helpful functions
    '''
    def __init__(self, yml_dict, name):
        super(StageConfig, self).__init__()
        required_keys=['fcl','n_jobs','events_per_job','input','output']
        for key in required_keys:
            if key not in yml_dict:
                raise StageConfigException(key, name)
        self.name = name
        self.yml_dict = yml_dict
        