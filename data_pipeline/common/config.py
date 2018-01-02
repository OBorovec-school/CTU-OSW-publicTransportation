import logging
import os

import yaml

DEFAULT_CONFIG_PATH_TO_ROOT = 'config/config.yml'
LOGGING_CONF = 'config/logging.yml'


class DPConfig:
    conf = None
    root_dir = None

    @classmethod
    def load(cls, config_path=DEFAULT_CONFIG_PATH_TO_ROOT):
        if config_path is None:
            config_path = DEFAULT_CONFIG_PATH_TO_ROOT
        try:
            cls.conf = yaml.load(open(config_path, 'r'))
        except Exception as e:
            raise FileNotFoundError('Invalid config file ' + str(e))
        cls.set_root_dir(cls.get_working_dir_opt())
        cls.logger = logging.getLogger('dp')

    @classmethod
    def check_input_config(cls):
        pass

    @classmethod
    def get_working_dir_opt(cls):
        return cls.conf['data_pipeline'].get('targeta', 'project')

    @classmethod
    def get_rdf_type(cls):
        return cls.conf['data_pipeline'].get('rdf_output', 'turtle')

    @classmethod
    def get_logging_level(cls):
        return cls.conf['logging'].get('level', 'WARN')

    @classmethod
    def get_schedule(cls):
        return cls.conf['data_pipeline'].get('scheduling', 'daily')

    @classmethod
    def set_root_dir(cls, working_dir_opt):
        if working_dir_opt == 'project':
            cls.root_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")
        elif working_dir_opt == 'current':
            cls.root_dir = os.getcwd()
        else:
            if os.path.isdir(working_dir_opt):
                cls.root_dir = working_dir_opt
            else:
                logging.getLogger('dp').error('Configuration "[data_pipeline][target]" is not valid.')
                cls.root_dir = os.getcwd()

    @classmethod
    def get_def_conf_path(cls):
        return DEFAULT_CONFIG_PATH_TO_ROOT


def logging_init():
    if not os.path.isfile(LOGGING_CONF):
        print("Could not load logging configuration at destination: "
              + str(os.path.join(os.getcwd(), LOGGING_CONF)))
        raise FileNotFoundError('Missing ' + str(LOGGING_CONF))
    logging.config.dictConfig(yaml.safe_load(open(LOGGING_CONF, 'r').read()))


