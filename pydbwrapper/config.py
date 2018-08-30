"""PyDBWrapper Configuration"""
import json
import os

import psycopg2
from DBUtils.PooledDB import PooledDB


class ConfigurationNotFoundError(Exception):
    """Configuration file was not found"""


class InvalidConfigurationError(Exception):
    """Configuration is not a valid json file"""


class Config(object):
    """Configuration object"""
    __instance = None

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

        self.print_sql = self.data.pop('print_sql') if 'print_sql' in self.data else False 
        
        self.pool = PooledDB(psycopg2, **self.data)

    @staticmethod
    def instance(configuration_file='/etc/pydbwrapper/config.json', config_dict=None):
        """Get singleton instance of Configuration"""
        if Config.__instance is None:
            Config.__instance = Config(configuration_file, config_dict)
        return Config.__instance
