import hashlib

from bs4 import BeautifulSoup
from dateutil import parser
from rdflib import URIRef
from rdflib.namespace import XSD

from data_pipeline.common.producer.rss_producer import RSSProducer
from data_pipeline.common.transformer.item_rdf_transformer import ItemToRDFTransformer
from data_pipeline.common.utils import *
from data_pipeline.public_transp import PT_PRAGUE_IRREGULARITY_NAME_SPACE, PT_PRAGUE_IRREGULARITY_URL


class PTPragueIrregularityCrawler(RSSProducer):
    RSS_URL = PT_PRAGUE_IRREGULARITY_URL
    LUIGI_OUTPUT_FILE = 'PTPragueIrregularityCrawler'
    NAME = 'PTPragueIrregularityCrawler'
    META_FILE = 'PTPragueIrregularityCrawler'


class PTPragueIrregRDF(ItemToRDFTransformer):
    NAME = 'PTPragueIrregRDF'
    NAMESPACE = PT_PRAGUE_IRREGULARITY_NAME_SPACE
    NAMESPACE_PREFIX = 'pragueIrr'
    LUIGI_OUTPUT_FILE = 'PTPragueIrregRDF'

    def requires(self):
        return PTPragueIrregularityCrawler(self.unique_param)

    def parse_item_to_graph(self, item, g, n):
        id = hashlib.md5(str(item).encode('utf-8')).hexdigest()

        # Parsing of direct subroot elements
        title = item.get('title', None)
        publish_date = parser.parse(item.get('published', None))
        link = item.get('link', None)

        # Parsing from content_encode part
        content = item.get('content', item.get('conetent_encoded'))[0]
        affected_lines = get_element_nonroot_xml(content.value, 'aff_lines').split(',')
        affected_types = BeautifulSoup(get_element_nonroot_xml(content.value, 'aff_line_types'),
                                              "lxml").text.split(',')
        categories = get_element_nonroot_xml(content.value, 'emergency_types').split(',')
        from_time = parser.parse(get_element_nonroot_xml(content.value, 'time_start')).replace(
            tzinfo=publish_date.tzinfo)
        to_time = parser.parse(get_element_nonroot_xml(content.value, 'time_final_stop')).replace(
            tzinfo=publish_date.tzinfo)

        record = n[id]
        g.add((record, RDF.type, n.TrafficChange))
        g.add((record, n.title, Literal(title, datatype=XSD.string)))
        g.add((record, n.published, Literal(publish_date, datatype=XSD.datetime)))
        g.add((record, n.link, URIRef(link)))
        add_bag_to_graph(g, record, n.affectedLines, affected_lines, XSD.string)
        add_bag_to_graph(g, record, n.affectedTypes, affected_types, XSD.string)
        add_bag_to_graph(g, record, n.classification, categories, XSD.string)
        g.add((record, n.starts, Literal(from_time, datatype=XSD.datetime)))
        g.add((record, n.ends, Literal(to_time, datatype=XSD.datetime)))
