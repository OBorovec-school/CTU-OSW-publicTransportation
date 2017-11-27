import logging.config

import luigi
import yaml

from data_pipeline.public_transp.Prague.changes.rdf_transform import PTPragueChangesRDF
from utils.structure import *


class DataPipeline(luigi.Task):
    def requires(self):
        get_tmp_folder()
        return [PTPragueChangesRDF()]

    def output(self):
        return [luigi.LocalTarget(get_tmp_file('ptChangesPrague.rdf'))]

    def run(self):
        for i in range(len(self.input())):
            with self.output()[i].open("w") as res_file, self.input()[i].open() as in_file:
                res_file.write(in_file.read())
            with self.output()[i].open("r") as res_file, open(get_result_file(os.path.basename(res_file.name)), 'a')  as final_file:
                final_file.write(res_file.read() + "\n")
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
