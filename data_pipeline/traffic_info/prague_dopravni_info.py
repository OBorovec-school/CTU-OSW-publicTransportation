from data_pipeline.common.producer.rss_producer import RSSProducer
from data_pipeline.common.transformer.dopravni_info_rss_to_rdf import DopravniInfoTransformRDF
from data_pipeline.traffic_info import TI_DOPRAVNI_INFO_PRAGUE, TI_DOPRAVNI_INFO_PRAGUE_NAME_SPACE


class TIDopravniInfoPragueCrawler(RSSProducer):
    RSS_URL = TI_DOPRAVNI_INFO_PRAGUE
    LUIGI_OUTPUT_FILE = 'TIPragueDopravInfoCrawler'
    NAME = 'TIPragueDopravInfoCrawler'
    META_FILE = 'TIPragueDopravInfoCrawler'


class TIPragueDIRDF(DopravniInfoTransformRDF):
    NAME = 'TIPragueDopravInfoRDF'
    NAMESPACE = TI_DOPRAVNI_INFO_PRAGUE_NAME_SPACE
    NAMESPACE_PREFIX = 'pragueDi'
    LUIGI_OUTPUT_FILE = 'TIPragueDopravInfoRDF'

    def requires(self):
        return TIDopravniInfoPragueCrawler(self.unique_param)