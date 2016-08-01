# coding=utf8
import ConfigParser
import os

this_dir, this_filename = os.path.split(__file__)
config_file_path = os.path.join(this_dir, '..', 'config.ini')
config = ConfigParser.ConfigParser()
config.read(config_file_path)


def get_hive_host():
    return config.get('hive_info', 'host')


def get_hive_port():
    return int(config.get('hive_info', 'port'))


def get_hive_auth_mechanism():
    return config.get('hive_info', 'authMechanism')


def get_hive_user():
    return config.get('hive_info', 'user')


def get_hive_password():
    return config.get('hive_info', 'password')


def get_hive_database():
    return config.get('hive_info', 'database')
