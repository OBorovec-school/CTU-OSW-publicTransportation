import hashlib
import logging
import pickle
from collections import defaultdict
from datetime import datetime

import luigi
import rdflib
from bs4 import BeautifulSoup
from dateutil import parser
from luigi.format import UTF8
from rdflib import Namespace, Literal, URIRef
from rdflib.namespace import FOAF, RDF, XSD

from data_pipeline import RDF_FORMAT
from data_pipeline.common.structure import get_tmp_file
from data_pipeline.public_transp.Prague import PT_PRAGUE_CHANGES_NAME_SPACE
from data_pipeline.public_transp.Prague.changes_crawler import PTPragueChangesCrawler

logger = logging.getLogger('dp')
unparseble_logging = logging.getLogger('unparseble')


class PTPragueChangesRDF(luigi.Task):
    def requires(self):
        return PTPragueChangesCrawler()

    def output(self):
        return luigi.LocalTarget(get_tmp_file(__class__.__name__), format=UTF8)

    def run(self):
        g = rdflib.Graph()
        if self.output().exists():
            g.parse(self.output().path, format=RDF_FORMAT)
        n = Namespace(PT_PRAGUE_CHANGES_NAME_SPACE)
        g.bind('pragueChanges', n)
        with self.input().open() as fin:
            new_changes = pickle.load(fin)
            for item in new_changes:
                try:
                    # Parsing of direct subroot elements
                    title = item.get('title', None)
                    publish_date = parser.parse(item.get('published', None))
                    link = item.get('link', None)

                    # Parsing from content_encode part
                    content = item.get('content', item.get('conetent_encoded'))[0]
                    affected_lines = self.__get_element_nonroot_xml(content.value, 'lines').split(',')
                    categories = self.__get_element_nonroot_xml(content.value, 'type').replace('&nbsp;', ' ').split(',')
                    from_date, to_date = self.__parse_duration(self.__get_element_nonroot_xml(content.value, 'duration'), publish_date)

                    # Parsing full text description
                    full_text_description = item.get('description', None)
                    affects_on_lines = self.__parse_affects(full_text_description, affected_lines)

                    # Adding to graph
                    id = hashlib.md5(str(item).encode('utf-8')).hexdigest()
                    record = n[id]
                    g.add((record, RDF.type, FOAF.TrafficChange))
                    g.add((record, FOAF.title, Literal(title, datatype=XSD.string)))
                    g.add((record, FOAF.published, Literal(publish_date, datatype=XSD.datetime)))
                    g.add((record, FOAF.link, URIRef(link)))
                    # g.add((record, FOAF.fullTextDescHTML, Literal(item.get('description', None))))
                    # g.add((record, FOAF.fullTextDesc, Literal(BeautifulSoup(item.get
                    affects = rdflib.BNode()
                    g.add((affects, RDF.type, RDF.Bag))
                    g.add((record, FOAF.affect, affects))
                    for line in affected_lines:
                        affect = rdflib.BNode()
                        g.add((affects, RDF.li, affect))
                        g.add((affect, FOAF.line, Literal(line, datatype=XSD.string)))
                        if line in affects_on_lines:
                            g.add((affect, FOAF.reason, Literal(affects_on_lines[line], datatype=XSD.string)))
                    categs = rdflib.BNode()
                    g.add((categs, RDF.type, RDF.Bag))
                    g.add((record, FOAF.catgory, categs))
                    for category in categories:
                        g.add((categs, RDF.li, Literal(category, datatype=XSD.string)))
                    duration = rdflib.BNode()
                    g.add((record, FOAF.duration, duration))
                    g.add((duration, FOAF.from_date, Literal(from_date, datatype=XSD.datetime)))
                    if to_date is not None:
                        g.add((duration, FOAF.to_date, Literal(to_date, datatype=XSD.datetime)))
                except Exception as e:
                    logger.warning('Could not parse item with content: ' + str(e))
                    unparseble_logging.error(str(item))
        g.serialize(destination=self.output().path, format=RDF_FORMAT)

    def __get_element_nonroot_xml(self, xml_text, el_name):
        return xml_text.split('<' + str(el_name) + '>')[1].split('</' + str(el_name) + '>')[0]

    def __parse_duration(self, duration, pub_date):
        duration = duration.split('do odvolání')[0].replace(' ','').split('-')
        if duration[0].startswith('dnes'):
            from_date = datetime.strptime(duration[0].replace('dnes', ''), "%H:%M")
            from_date = from_date.replace(day=pub_date.day, month=pub_date.month)
        else:
            if len(duration[0]) > 6:
                from_date = datetime.strptime(duration[0], "%d.%m.%H:%M")
            else:
                from_date = datetime.strptime(duration[0], "%d.%m.")
        from_date = from_date.replace(year=pub_date.year, tzinfo=pub_date.tzinfo)
        if from_date < pub_date: # in case of the end of a year
            from_date = from_date.replace(year=pub_date.year+1)
        to_date = None
        if len(duration) > 1:
            to_date = datetime.strptime(duration[1], "%d.%m.%H:%M") if len(duration[1]) > 5 else datetime.strptime(duration[1], "%H:%M")
            to_date = to_date.replace(year=from_date.year, tzinfo=from_date.tzinfo)
        return from_date, to_date

    def __parse_affects(self, description, affected_lines):
        affects_on_lines = defaultdict()
        soup = BeautifulSoup(description, "lxml")
        for el in soup.findAll('li'):
            if str(el).startswith('<li>Link'):  # can be '<li>Linky' or '<li>Linka':
                for line in [x.text for x in el.findAll('strong') if x.text in affected_lines]:
                    affects_on_lines[line] = ' '.join(
                        [x.text for x in el.findAll('strong') if x.text not in affected_lines])
        return affects_on_lines