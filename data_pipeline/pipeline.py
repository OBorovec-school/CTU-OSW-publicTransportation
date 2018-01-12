import logging.config
import subprocess
import sys
from time import strftime, gmtime

from data_pipeline.common.config import logging_init, DPConfig
from data_pipeline.common.luigid import luigid_running
from data_pipeline.common.sink.text_rdf_merge import TextRDFMerge
from data_pipeline.public_transp.brno_changes import PTBrnoChangesRDF
from data_pipeline.public_transp.pilsen_changes import PTPilsenChangesRDF
from data_pipeline.public_transp.prague_changes import PTPragueChangesRDF
from data_pipeline.public_transp.prague_irregularity import PTPragueIrregRDF
from data_pipeline.traffic_info.brno_dopravni_info import TIBrnoDIRDF
from data_pipeline.traffic_info.prague_dopravni_info import TIPragueDIRDF


class DataPipeline(TextRDFMerge):
    NAME = 'DataPipeline'
    MAPPING = [(PTPragueChangesRDF, 'ptChangesPrague.rdf'),
               (PTPragueIrregRDF, 'ptIrregPrague.rdf'),
               (PTBrnoChangesRDF, 'ptChangesBrno.rdf'),
               (PTPilsenChangesRDF, 'ptChangesPilsen.rdf'),
               (TIPragueDIRDF, 'tiPrague.rdf'),
               (TIBrnoDIRDF, 'tiBrno.rdf')]


if __name__ == "__main__":
    logging_init()

    conf_path = None
    if len(sys.argv) == 2:
        conf_path = sys.argv[1]
    DPConfig.load(conf_path)

    logger = logging.getLogger('dp')
    logging.getLogger().setLevel(DPConfig.get_logging_level())
    logger.info('Test run')
    date_param = strftime("%Y-%m-%d-%H-%M-%S", gmtime())
    logging.getLogger('dp').info('Starting data pipeline at ' + date_param + '.')
    subprocess_input = ["luigi",
                        "--module", "data_pipeline.pipeline", "DataPipeline",
                        "--unique-param", date_param,
                        "--config-path", DPConfig.get_def_conf_path()]
    if not DPConfig.get_luigid() or not luigid_running():
        subprocess_input.append("--local-scheduler")
    process = subprocess.Popen(subprocess_input)
    logging.getLogger('dp_pid').info('Started Luigi process with pid: ' + str(process.pid))
    (output, err) = process.communicate()
    p_status = process.wait()