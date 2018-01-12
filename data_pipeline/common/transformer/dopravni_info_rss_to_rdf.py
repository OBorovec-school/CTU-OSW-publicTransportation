import hashlib

from dateutil import parser
from rdflib import RDF, XSD, Literal, URIRef

from data_pipeline.common.transformer.item_rdf_transformer import ItemToRDFTransformer


class DopravniInfoTransformRDF(ItemToRDFTransformer):
    def parse_item_to_graph(self, item, g, n):
        id = hashlib.md5(str(item).encode('utf-8')).hexdigest()

        # Parsing of direct subroot elements
        title = item.get('title', None)
        publish_date = parser.parse(item.get('published', None))
        link = item.get('link', None)
        description = item.get('summary', None)
        classification = title.split(':')[-1].replace(' ', '', 1)
        internal_id = item.get('id', None)

        record = n[id]
        g.add((record, RDF.type, n.TrafficInfo))
        g.add((record, n.title, Literal(title, datatype=XSD.string)))
        g.add((record, n.published, Literal(publish_date, datatype=XSD.datetime)))
        g.add((record, n.link, URIRef(link)))
        g.add((record, n.description, Literal(description, datatype=XSD.string)))
        g.add((record, n.classification, Literal(classification, datatype=XSD.string)))
        g.add((record, n.internalId, Literal(internal_id, datatype=XSD.string)))