import logging
import subprocess

import os
import urllib

from data_pipeline.common.structure import get_log_folder

LUIGID_PID = 'luigid.pid'
LUIGID_STATE_FILE = 'luigid.state'




def luigid_running():
    try:
        urllib.request.urlopen('http://localhost:8082').info()
        return True
    except:
        return False


def run_luigid(log_dir):
    process = subprocess.Popen(["luigid",
                                "--logdir" , str(log_dir)])
    logging.getLogger('dp_pid').info('Started Luigid with pid: ' + str(process.pid))

