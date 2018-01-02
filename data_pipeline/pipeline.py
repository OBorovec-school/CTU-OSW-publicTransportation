import logging.config
import os
import sys
import subprocess
from time import strftime, gmtime

import luigi
import rdflib
from luigi.format import UTF8

from data_pipeline.common.config import logging_init, DPConfig
from data_pipeline.common.sink.text_rdf_merge import TextRDFMerge
from data_pipeline.common.structure import get_tmp_folder, get_tmp_file, get_result_file, clear_tmp_folder
from data_pipeline.public_transp.Brno.html_els_rdf_transformer import PTBrnoChangesRDF
from data_pipeline.public_transp.Prague.changes_rdf_transform import PTPragueChangesRDF
from data_pipeline.public_transp.Prague.irreg_rdf_transform import PTPragueIrregRDF


class DataPipeline(TextRDFMerge):
    NAME = 'DataPipeline'
    MAPPING = [(PTPragueChangesRDF, 'ptChangesPrague.rdf'),
               (PTPragueIrregRDF, 'ptIrregPrague.rdf'),
               (PTBrnoChangesRDF, 'ptChangesBrno.rdf')]


def run_pipeline():
    date_param = strftime("%Y-%m-%d-%H-%M-%S", gmtime())
    logging.getLogger('dp').info('Starting data pipeline at ' + date_param + '.')
    process = subprocess.Popen(["luigi",
                                "--module", "data_pipeline.pipeline", "DataPipeline",
                                "--unique-param" , 'date_param'])
    logging.getLogger('dp_pid').info('Started Luigi process with pid: ' + str(process.pid))
    '''
    luigi.run(
        cmdline_args=["--unique-param=" + date_param],
        main_task_cls=DataPipeline
        #, local_scheduler = True
    )
    '''


if __name__ == "__main__":
    logging_init()

    conf_path = None
    if len(sys.argv) == 2:
        conf_path = sys.argv[1]
    DPConfig.load(conf_path)

    logger = logging.getLogger('dp')
    logging.getLogger().setLevel(DPConfig.get_logging_level())
    logger.info('Test run')
