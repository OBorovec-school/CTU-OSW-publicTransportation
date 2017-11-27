import os
import shutil

from utils.constants import *


def get_main_folder(folder_name):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), "../" + folder_name)


def create_main_level_folder(folder_name):
    folder = get_main_folder(folder_name)
    if not os.path.exists(folder):
        os.makedirs(folder)


def get_meta_folder():
    create_main_level_folder(META_FOLDER)
    return get_main_folder(META_FOLDER)


def get_log_folder():
    create_main_level_folder(LOG_FOLDER)
    return get_main_folder(LOG_FOLDER)


def get_result_folder():
    create_main_level_folder(RESULT_FOLDER)
    return get_main_folder(RESULT_FOLDER)


def get_tmp_folder():
    create_main_level_folder(TMP_FOLDER)
    return get_main_folder(TMP_FOLDER)


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
