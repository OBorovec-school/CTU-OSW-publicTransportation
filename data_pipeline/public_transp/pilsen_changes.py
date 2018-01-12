import hashlib

from dateutil import parser
from rdflib import Literal, URIRef
from rdflib.namespace import XSD

from data_pipeline.common.producer.rss_producer import RSSProducer
from data_pipeline.common.transformer.item_rdf_transformer import ItemToRDFTransformer
from data_pipeline.public_transp import PT_PILSEN_CHANGES_URL, PT_PILSEN_CHANGES_NAME_SPACE


class PTPilsenChangesCrawler(RSSProducer):
    RSS_URL = PT_PILSEN_CHANGES_URL
    LUIGI_OUTPUT_FILE = 'PTPilsenChangesCrawler'
    NAME = 'PTPilsenChangesCrawler'
    META_FILE = 'PTPilsenChangesCrawler'


class PTPilsenChangesRDF(ItemToRDFTransformer):
    NAME = 'PTPilsenChangesRDF'
    NAMESPACE = PT_PILSEN_CHANGES_NAME_SPACE
    NAMESPACE_PREFIX = 'pilsenChanges'
    LUIGI_OUTPUT_FILE = 'PTPilsenChangesRDF'

    def requires(self):
        return PTPilsenChangesCrawler(self.unique_param)

    def parse_item_to_graph(self, item, g, n):
        # Adding to graph
        id = hashlib.md5(str(item).encode('utf-8')).hexdigest()

        # Parsing of direct subroot elements
        title = item.get('title', None)
        publish_date = parser.parse(item.get('published', None))
        link = item.get('link', None)
        summary = item.get('summary', None)

        record = n[id]
        g.add((record, n.title, Literal(title, datatype=XSD.string)))
        g.add((record, n.published, Literal(publish_date, datatype=XSD.datetime)))
        g.add((record, n.link, URIRef(link)))
