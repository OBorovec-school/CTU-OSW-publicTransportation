import logging.config

import luigi
import rdflib
import yaml
from luigi.format import UTF8

from data_pipeline.common.structure import *
from data_pipeline.public_transp.Brno.html_els_rdf_transformer import PTBrnoChangesRDF
from data_pipeline.public_transp.Prague.changes_rdf_transform import PTPragueChangesRDF
from data_pipeline.public_transp.Prague.irreg_rdf_transform import PTPragueIrregRDF


class DataPipeline(luigi.Task):

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger('dp')
        self.rdf_output = yaml.load(open("config.yml", 'r'))['data_pipeline']['rdf_output']

    def requires(self):
        get_tmp_folder()
        return [PTPragueChangesRDF(),
                PTPragueIrregRDF(),
                PTBrnoChangesRDF()]

    def output(self):
        return [luigi.LocalTarget(get_tmp_file('ptChangesPrague.rdf'), format=UTF8),
                luigi.LocalTarget(get_tmp_file('ptIrregPrague.rdf'), format=UTF8),
                luigi.LocalTarget(get_tmp_file('ptChangesBrno.rdf'), format=UTF8)]

    def run(self):
        for i in range(len(self.input())):
            # Onpy copy of input if it would be needed for further steps of pipeline
            with self.output()[i].open('w') as out_file, self.input()[i].open() as in_file:
                out_file.write(in_file.read())
            # merge current input graph with result file graph
            result_file_path = get_result_file(os.path.basename(self.output()[i].path))
            g = rdflib.Graph()
            if os.path.isfile(result_file_path):
                g.parse(result_file_path, format=self.rdf_output)
            g.parse(self.input()[i].path, format=self.rdf_output)
            g.serialize(destination=result_file_path, format=self.rdf_output)
        clear_tmp_folder()


if __name__ == "__main__":
    init_structure()
    logging.basicConfig(filename='log/startup.log', level='INFO')

    logging.info('Starting...')

    logging.info('Loading configuration.')
    cfg = yaml.load(open("config.yml", 'r'))

    logging.info('Setting logging.')
    conf_path = 'logging.yml'
    level = cfg['logging']['level']
    if os.path.exists(conf_path):
        with open(conf_path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
        logging.getLogger().setLevel(level)
    else:
        logging.warning('Logging configuration not found. Using basicconfiguration with level WARN.')
        logging.basicConfig(filename='log/runtime.log', level='WARN')

    logging.getLogger('dp').info('_____________________________________')
    logging.getLogger('dp').info('Starting procedures of data pipeline.')
    luigi.run()
