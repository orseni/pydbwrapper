"""PyDBWrapper Configuration"""
import os
import json
import psycopg2

class ConfigurationNotFoundError(Exception):
    """Configuration file was not found"""


class InvalidConfigurationError(Exception):
    """Configuration is not a valid json file"""


class Config(object):
    """Configuration object"""
    __instance = None
    pool = None


    def __init__(self, configuration_file=None, config_dict=None):
        if configuration_file:
            if not os.path.exists(configuration_file):
                raise ConfigurationNotFoundError()
            with open(configuration_file, 'r') as config:
                try:
                    self.data = json.loads(config.read())
                except json.decoder.JSONDecodeError as ex:
                    raise InvalidConfigurationError(ex)
        elif config_dict:
            self.data = config_dict
        self.pool = psycopg2.pool.SimpleConnectionPool(50, 50, **self.data)

    def getconn():
        return self.pool.getconn()

    @staticmethod
    def instance(configuration_file='/etc/pydbwrapper/config.json'):
        """Get singleton instance of Configuration"""
        if Config.__instance is None:
            Config.__instance = Config(configuration_file)
        return Config.__instance
