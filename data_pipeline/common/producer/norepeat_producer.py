import hashlib
import logging
import os
import pickle

import luigi
from luigi.format import UTF8

from data_pipeline.common.structure import get_meta_file, get_tmp_file


class NoRepeatProducer(luigi.Task):
    NAME = NotImplementedError
    META_FILE = NotImplementedError
    LUIGI_OUTPUT_FILE = NotImplementedError

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger('dp')
        self.last_entries = set()
        if os.path.isfile(get_meta_file(self.META_FILE)):
            self.last_entries = pickle.load(open(get_meta_file(self.META_FILE), 'rb'))

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(get_tmp_file(self.LUIGI_OUTPUT_FILE), format=luigi.format.Nop)

    def run(self):
        self.logger.info('Running: ' + self.NAME)
        content = self.get_full_content()
        entries = self.get_entries(content)
        with self.output().open('wb') as fout:
            pickle.dump([entry for entry in entries
                         if hashlib.md5(str(entry).encode('utf-8')).hexdigest() not in self.last_entries],
                        fout, pickle.HIGHEST_PROTOCOL)
        self.last_entries = set([hashlib.md5(str(entry).encode('utf-8')).hexdigest() for entry in entries])
        with open(get_meta_file(self.META_FILE), 'wb') as content_file:
            pickle.dump(self.last_entries, content_file, pickle.HIGHEST_PROTOCOL)

    def get_entries(self, content):
        raise NotImplementedError

    def get_full_content(self):
        raise NotImplementedError