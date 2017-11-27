import logging

import luigi
import pickle

from data_pipeline.public_transp.Prague.changes.crawler import PTPragueChangesCrawler
from utils.structure import get_tmp_file

logger = logging.getLogger('dp')


class PTPragueChangesRDF(luigi.Task):

    def requires(self):
        return PTPragueChangesCrawler()

    def output(self):
        return luigi.LocalTarget(get_tmp_file(__class__.__name__))

    def run(self):
        with self.input().open() as fin, self.output().open('w') as fout:
            inputX = pickle.load(fin)
            logger.info("Found: " +str(len(inputX)))
            fout.write('Done')
