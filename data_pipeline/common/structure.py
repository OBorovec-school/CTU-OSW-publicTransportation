import logging
import os
import shutil

import yaml

META_FOLDER_NAME = 'META'
LOG_FOLDER_NAME = 'log'
RESULT_FOLDER_NAME = 'result'
TMP_FOLDER_NAME = 'tmp'

MAIN_FOLDER_OPTION = yaml.load(open("config.yml", 'r'))['data_pipeline']['target']


def get_root_level_folder(folder_name):
    if MAIN_FOLDER_OPTION == 'project':
        root_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../" + folder_name)
    elif MAIN_FOLDER_OPTION == 'current':
        root_folder = os.getcwd()
    else:
        if os.path.isdir(MAIN_FOLDER_OPTION):
            root_folder = MAIN_FOLDER_OPTION
        else:
            logging.getLogger('dp').error('Configuration "[data_pipeline][target]" is not valid.')
            root_folder = os.getcwd()
    return root_folder


def create_subroot_level_folder(folder_name):
    folder = get_root_level_folder(folder_name)
    if not os.path.exists(folder):
        os.makedirs(folder)


def get_meta_folder():
    create_subroot_level_folder(META_FOLDER_NAME)
    return get_root_level_folder(META_FOLDER_NAME)


def get_log_folder():
    create_subroot_level_folder(LOG_FOLDER_NAME)
    return get_root_level_folder(LOG_FOLDER_NAME)


def get_result_folder():
    create_subroot_level_folder(RESULT_FOLDER_NAME)
    return get_root_level_folder(RESULT_FOLDER_NAME)


def get_tmp_folder():
    create_subroot_level_folder(TMP_FOLDER_NAME)
    return get_root_level_folder(TMP_FOLDER_NAME)


def clear_tmp_folder():
    shutil.rmtree(get_tmp_folder())


def get_meta_file(file_name):
    return os.path.join(get_meta_folder(), file_name)


def get_result_file(file_name):
    return os.path.join(get_result_folder(), file_name)


def get_tmp_file(file_name):
    return os.path.join(get_tmp_folder(), file_name)


def init_structure():
    get_log_folder()
    get_meta_folder()

