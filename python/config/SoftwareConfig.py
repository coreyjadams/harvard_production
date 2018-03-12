

from ConfigException import ConfigException

class SoftwareConfigException(ConfigException):
    '''Specialized exception for configuration errors with larsoft block'''

    def __init__(self, message = None, key = None):
        message = 'Error Configuring Larsoft'
        if key is not None:
            message += ': Missing keyword {}'.format(key)
        super(SoftwareConfigException, self).__init__(message)


class SoftwareConfig(object):
    """docstring for SoftwareConfig"""
    def __init__(self):
        super(SoftwareConfig, self).__init__()


    def setup(self, return_env=False):
        raise NotImplementedError("Required to implement the setup function.")

