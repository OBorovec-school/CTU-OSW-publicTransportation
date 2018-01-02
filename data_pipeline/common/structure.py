import logging
import os
import shutil

from data_pipeline.common.config import DPConfig

META_FOLDER_NAME = 'META'
LOG_FOLDER_NAME = 'log'
RESULT_FOLDER_NAME = 'result'
TMP_FOLDER_NAME = 'tmp'


def get_subroot_level_folder(folder_name):
    folder = os.path.join(DPConfig.root_dir, folder_name)
    if not os.path.exists(folder):
        os.makedirs(folder)
    return folder


def get_meta_folder():
    return get_subroot_level_folder(META_FOLDER_NAME)
def get_log_folder():
    return get_subroot_level_folder(LOG_FOLDER_NAME)
def get_result_folder():
    return get_subroot_level_folder(RESULT_FOLDER_NAME)
def get_tmp_folder():
    return get_subroot_level_folder(TMP_FOLDER_NAME)

def get_meta_file(file_name):
    return os.path.join(get_meta_folder(), file_name)
def get_result_file(file_name):
    return os.path.join(get_result_folder(), file_name)
def get_tmp_file(file_name):
    return os.path.join(get_tmp_folder(), file_name)

def clear_tmp_folder():
    shutil.rmtree(get_tmp_folder())


