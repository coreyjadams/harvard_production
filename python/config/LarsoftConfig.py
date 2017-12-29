
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
        