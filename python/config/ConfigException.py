
class ConfigException(Exception):
    """Specialized exception for configuration classes"""

    def __init__(self, message):
        super(ConfigException, self).__init__(message)
        