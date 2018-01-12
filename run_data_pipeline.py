import logging

import schedule as schedule
import subprocess

import time

import sys

from data_pipeline.common.config import DPConfig
from data_pipeline.common.luigid import run_luigid, luigid_running
from data_pipeline.common.structure import get_log_folder
from data_pipeline.pipeline import run_init


def run():
    logging.getLogger('dp').error('Running scheduled task.')
    date_param = time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime())
    logging.getLogger('dp').info('Starting data pipeline at ' + date_param + '.')
    process = subprocess.Popen(["luigi",
                                "--module", "data_pipeline.pipeline", "DataPipeline",
                                "--unique-param", date_param,
                                "--config-path", DPConfig.get_def_conf_path()])
    logging.getLogger('dp_pid').info('Started Luigi process with pid: ' + str(process.pid))


def schedule_tasks(schedule_opt):
    if schedule_opt == 'weekly':
        schedule.every().week.at('00:00').do(run)
    elif schedule_opt == 'daily':
        schedule.every().day.at('00:00').do(run)
    elif schedule_opt == 'hourly':
        schedule.every().hour.at().do(run)
    elif schedule_opt == 'test':
        schedule.every().minute.do(run)


if __name__ == '__main__':
    conf_path = None
    if len(sys.argv) == 2:
        conf_path = sys.argv[1]
    run_init(conf_path)
    logging.getLogger('dp').info('Pinging to Luigid server.')
    if not luigid_running():
        run_luigid(get_log_folder())
    logging.getLogger('dp').info('Data pipeline has been started.')
    schedule_opt = DPConfig.get_schedule()
    schedule_tasks(schedule_opt)
    run()
    while True:
        schedule.run_pending()
        time.sleep(1)