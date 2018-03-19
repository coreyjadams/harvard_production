

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
        self.yml_dict = dict()

    def setup(self, return_env=False):
        raise NotImplementedError("Required to implement the setup function.")



    def __getitem__(self, key):
        '''return objects

        Returns yml parameters, and returns defaults for some parameters if they
        are not specified in the file

        Arguments:
            key {key} -- dictionary key, typcially string

        Returns:
            object -- value for the key
        '''
        # List of defaults:

        return self.yml_dict[key]