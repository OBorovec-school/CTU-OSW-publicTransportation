from data_pipeline.common.producer.rss_producer import RSSProducer
from data_pipeline.public_transp.Prague import PT_PRAGUE_IRREGULARITY_URL


class PTPragueIrregularityCrawler(RSSProducer):
    RSS_URL = PT_PRAGUE_IRREGULARITY_URL
    LUIGI_OUTPUT_FILE = 'PTPragueIrregularityCrawler'
    NAME = 'PTPragueIrregularityCrawler'
    META_FILE = 'PTPragueIrregularityCrawler'
