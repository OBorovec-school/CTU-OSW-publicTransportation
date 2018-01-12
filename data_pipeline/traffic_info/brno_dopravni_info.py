from data_pipeline.common.producer.rss_producer import RSSProducer
from data_pipeline.common.transformer.dopravni_info_rss_to_rdf import DopravniInfoTransformRDF
from data_pipeline.traffic_info import TI_DOPRAVNI_INFO_BRNO, TI_DOPRAVNI_INFO_BRNO_NAME_SPACE


class TIDopravniInfoBrnoCrawler(RSSProducer):
    RSS_URL = TI_DOPRAVNI_INFO_BRNO
    LUIGI_OUTPUT_FILE = 'TIBrnoDopravInfoCrawler'
    NAME = 'TIBrnoDopravInfoCrawler'
    META_FILE = 'TIBrnoDopravInfoCrawler'


class TIBrnoDIRDF(DopravniInfoTransformRDF):
    NAME = 'TIBrnoDopravInfoRDF'
    NAMESPACE = TI_DOPRAVNI_INFO_BRNO_NAME_SPACE
    NAMESPACE_PREFIX = 'BrnoDi'
    LUIGI_OUTPUT_FILE = 'TIBrnoDopravInfoRDF'

    def requires(self):
        return TIDopravniInfoBrnoCrawler(self.unique_param)