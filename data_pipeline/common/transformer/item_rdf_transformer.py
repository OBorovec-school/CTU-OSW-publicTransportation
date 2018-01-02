import logging
import pickle

import luigi
import rdflib
from luigi.format import UTF8
from rdflib import Namespace

from data_pipeline.common.config import DPConfig
from data_pipeline.common.structure import get_tmp_file

logger = logging.getLogger('dp')
unparseble_logging = logging.getLogger('unparseble')


class ItemToRDFTransformer(luigi.Task):
    NAME = NotImplementedError
    NAMESPACE = NotImplementedError
    NAMESPACE_PREFIX = NotImplementedError
    LUIGI_OUTPUT_FILE = NotImplementedError

    unique_param = luigi.Parameter()

    def __init__(self, unique_param):
        super().__init__(unique_param)
        self.logger = logging.getLogger('dp')
        self.unparseble_logger = logging.getLogger('unparseble')
        self.rdf_output = DPConfig.get_rdf_type()

    def requires(self):
        raise NotImplementedError

    def output(self):
        output_file_name = self.LUIGI_OUTPUT_FILE + '_' + self.unique_param
        return luigi.LocalTarget(get_tmp_file(output_file_name), format=UTF8)

    def run(self):
        g = rdflib.Graph()
        if self.output().exists():
            g.parse(self.output().path, format=self.rdf_output)
        n = Namespace(self.NAMESPACE)
        g.bind(self.NAMESPACE_PREFIX, n)
        with self.input().open() as fin:
            for item in pickle.load(fin):
                try:
                    self.parse_item_to_graph(item, g, n)
                except Exception as e:
                    logger.warning('Could not parse item with content: ' + str(e))
                    unparseble_logging.error(str(item))
        g.serialize(destination=self.output().path, format=self.rdf_output)

    def parse_item_to_graph(self, item, g, n):
        raise NotImplementedError
