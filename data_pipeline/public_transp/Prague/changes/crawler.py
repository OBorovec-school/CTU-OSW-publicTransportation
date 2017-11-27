import logging
import pickle
import hashlib

import feedparser
import luigi
import os

from data_pipeline.public_transp.data_sources import PT_PRAGUE_CHANGES_URL
from utils.structure import get_meta_file, get_tmp_file

SUFFIX_OUTPUT = '_lastOutput'
SUFFIX_LAST_CONTENT = '_lastContent'


logger = logging.getLogger('dp')


class PTPragueChangesCrawler(luigi.Task):

    def __init__(self):
        super().__init__()
        self.last_content = set()
        if os.path.isfile(get_meta_file(__class__.__name__ + SUFFIX_LAST_CONTENT)):
            self.last_content = pickle.load(open(get_meta_file(__class__.__name__ + SUFFIX_LAST_CONTENT), 'rb'))

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(
            get_tmp_file(__class__.__name__ + SUFFIX_OUTPUT),
            format=luigi.format.Nop)

    def run(self):
        logger.info('Running: ' + __class__.__name__)
        feed = feedparser.parse(PT_PRAGUE_CHANGES_URL)
        with self.output().open('wb') as fout:
            pickle.dump([entry for entry in feed['entries']
                         if hashlib.md5((entry['title'] + entry['published']).encode('utf-8')).hexdigest() not in self.last_content],
                        fout, pickle.HIGHEST_PROTOCOL)
        self.last_content = set([hashlib.md5((entry['title'] + entry['published']).encode('utf-8')).hexdigest() for entry in feed['entries']])
        with open(get_meta_file(__class__.__name__ + SUFFIX_LAST_CONTENT), 'wb') as content_file:
            pickle.dump(self.last_content, content_file, pickle.HIGHEST_PROTOCOL)
