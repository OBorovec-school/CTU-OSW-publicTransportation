import logging
import pickle

import luigi
import rdflib
import yaml
from luigi.format import UTF8
from rdflib import Namespace

from data_pipeline import RDF_FORMAT
from data_pipeline.common.structure import get_tmp_file

logger = logging.getLogger('dp')
unparseble_logging = logging.getLogger('unparseble')


class ItemToRDFTransformer(luigi.Task):
    NAME = NotImplementedError
    NAMESPACE = NotImplementedError
    NAMESPACE_PREFIX = NotImplementedError
    LUIGI_OUTPUT_FILE = NotImplementedError

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger('dp')
        self.unparseble_logger = logging.getLogger('unparseble')
        self.rdf_output = yaml.load(open("config.yml", 'r'))['data_pipeline']['rdf_output']

    def requires(self):
        raise NotImplementedError

    def output(self):
        return luigi.LocalTarget(get_tmp_file(self.LUIGI_OUTPUT_FILE), format=UTF8)

    def run(self):
        g = rdflib.Graph()
        if self.output().exists():
            g.parse(self.output().path, format=RDF_FORMAT)
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
