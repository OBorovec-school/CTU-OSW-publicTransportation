import logging
import os
from time import strftime, gmtime

import luigi
import rdflib
from luigi.format import UTF8

from data_pipeline.common.config import DPConfig
from data_pipeline.common.structure import get_result_file


class TextRDFMerge(luigi.Task):
    NAME = NotImplementedError
    MAPPING = NotImplementedError

    unique_param = luigi.Parameter(
        default=strftime("%Y-%m-%d-%H-%M-%S", gmtime())
    )
    config_path = luigi.Parameter(default=None)

    def __init__(self, unique_param, config_path):
        super().__init__(unique_param, config_path)
        DPConfig.load(self.config_path)
        self.logger = logging.getLogger('dp')
        self.rdf_output = DPConfig.get_rdf_type()

    def complete(self):
        return False

    def requires(self):
        return [item[0](self.unique_param) for item in self.MAPPING]

    def output(self):
        return [luigi.LocalTarget(get_result_file(item[1]), format=UTF8) for item in self.MAPPING]

    def run(self):
        for pair in list(zip(self.input(), self.output())):
            input = pair[0]
            output = pair[1]
            g = rdflib.Graph()
            if os.path.isfile(output.path):
                g.parse(output.path, format=self.rdf_output)
            g.parse(input.path, format=self.rdf_output)
            g.serialize(destination=output.path, format=self.rdf_output)
