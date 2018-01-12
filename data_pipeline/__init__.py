import logging
import sys

from data_pipeline.common.config import logging_init, DPConfig
from data_pipeline.common.structure import get_log_folder


BASE_NAMESPACE = 'http://publictransportation.cz/'

def run_init():
    conf_path = None
    if len(sys.argv) == 2:
        conf_path = sys.argv[1]
    DPConfig.load(conf_path)

    get_log_folder()
    logging_init()
    logging.getLogger().setLevel(DPConfig.get_logging_level())
