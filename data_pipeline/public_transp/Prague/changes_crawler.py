from data_pipeline.common.producer.rss_producer import RSSProducer
from data_pipeline.public_transp.Prague import PT_PRAGUE_CHANGES_URL


class PTPragueChangesCrawler(RSSProducer):
    RSS_URL = PT_PRAGUE_CHANGES_URL
    LUIGI_OUTPUT_FILE = 'PTPragueChangesCrawler'
    NAME = 'PTPragueChangesCrawler'
    META_FILE = 'PTPragueChangesCrawler'
