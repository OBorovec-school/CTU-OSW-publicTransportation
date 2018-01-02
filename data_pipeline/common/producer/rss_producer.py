import feedparser

from data_pipeline.common.producer.norepeat_producer import NoRepeatProducer


class RSSProducer(NoRepeatProducer):
    NAME = NotImplementedError
    RSS_URL = NotImplementedError
    META_FILE = NotImplementedError
    LUIGI_OUTPUT_FILE = NotImplementedError

    def get_full_content(self):
        return feedparser.parse(self.RSS_URL)

    def get_entries(self, feed):
        return feed['entries']
