import hashlib
import logging
import os
import pickle

import feedparser
import luigi
from luigi.format import UTF8

from data_pipeline.common.structure import get_meta_file, get_tmp_file
from data_pipeline.public_transp.Prague import PT_PRAGUE_CHANGES_URL

logger = logging.getLogger('dp')


class PTPragueChangesCrawler(luigi.Task):

    def __init__(self):
        super().__init__()
        self.last_content = set()
        if os.path.isfile(get_meta_file(__class__.__name__)):
            self.last_content = pickle.load(open(get_meta_file(__class__.__name__), 'rb'))

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(get_tmp_file(__class__.__name__), format=luigi.format.Nop)

    def run(self):
        logger.info('Running: ' + __class__.__name__)
        feed = feedparser.parse(PT_PRAGUE_CHANGES_URL)
        with self.output().open('wb') as fout:
            pickle.dump([entry for entry in feed['entries']
                         if hashlib.md5(str(entry).encode('utf-8')).hexdigest() not in self.last_content],
                        fout, pickle.HIGHEST_PROTOCOL)
        self.last_content = set([hashlib.md5(str(entry).encode('utf-8')).hexdigest() for entry in feed['entries']])
        with open(get_meta_file(__class__.__name__), 'wb') as content_file:
            pickle.dump(self.last_content, content_file, pickle.HIGHEST_PROTOCOL)
