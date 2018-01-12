import hashlib

from bs4 import BeautifulSoup
from dateutil import parser
from rdflib import RDF, Literal
from rdflib.namespace import XSD

from data_pipeline.common.producer.web_page_producer import WebPageProducer
from data_pipeline.common.transformer.item_rdf_transformer import ItemToRDFTransformer
from data_pipeline.common.utils import add_bag_to_graph
from data_pipeline.public_transp import PT_BRNO_CHANGES_NAME_SPACE, PT_BRNO_CHANGES_URL


class BrnoChangesWebCrawler(WebPageProducer):

    NAME = 'PTBrnoChangesCrawler'
    WEB_URL = PT_BRNO_CHANGES_URL
    META_FILE = 'PTBrnoChangesCrawler'
    LUIGI_OUTPUT_FILE = 'PTBrnoChangesCrawler'

    def get_entries(self, html_content):
        bs = BeautifulSoup(html_content, "lxml")
        main_info = bs.findAll("div", {"class": ["selInfo"]})
        side_info = bs.findAll("div", {"class": ["selInfo1"]})
        if len(main_info) != len(side_info):
            # log warning - not expected
            pass
        related_divs = list(zip(main_info, side_info))
        entry_str = list(map(lambda x: (str(x[0]), str(x[1])), related_divs)) # Otherwise pickle dump fails due to RecursionError
        return entry_str


class PTBrnoChangesRDF(ItemToRDFTransformer):
    NAME = 'PTBrnoChangesRDF'
    NAMESPACE = PT_BRNO_CHANGES_NAME_SPACE
    NAMESPACE_PREFIX = 'brnoChanges'
    LUIGI_OUTPUT_FILE = 'PTBrnoChangesRDF'

    def requires(self):
        return BrnoChangesWebCrawler(self.unique_param)

    def parse_item_to_graph(self, item, g, n):
        id = hashlib.md5(str(item).encode('utf-8')).hexdigest()
        main_info = BeautifulSoup(item[0], "lxml")
        side_info = BeautifulSoup(item[1], "lxml")

        title = main_info.find('div', {'class': 'boxTxt truncNps'}).text
        affected_lines = main_info.find('div', {'class': 'boxLink truncLnk'}).text.replace('Linky: ', '').strip().split(
            ',')
        str_times = main_info.find('div', {'class': 'boxOdDo truncDate'}).text.replace('Od: ', '').split('Do: ')
        time = list(map(lambda x: parser.parse(x), str_times))
        main_category = main_info.find('div', {'class': 'boxTyp'}).find('img')['alt']
        category = [main_category]

        if main_category == 'Dopravní informace':
            pass
        elif main_category == 'Mimořádná událost v MHD':
            category.append(title.split('-')[0].strip())
            more_info = side_info.get_text(strip=True, separator="$").split('$')
            stop = more_info[0].replace('Zastávka: ', '')
            direction = more_info[1].replace('Směr: ', '')
            reason = more_info[2].replace('Důvod: ', '')
            delay = more_info[3].replace('Zdržení: ', '')
            sug_solution = more_info[4]
        elif main_category == 'Výluka v MHD':
            pass
        else:
            # Log unknown category
            pass

        record = n[id]
        g.add((record, RDF.type, n.TrafficChange))
        g.add((record, n.title, Literal(title, datatype=XSD.string)))
        g.add((record, n.starts, Literal(time[0], datatype=XSD.datetime)))
        g.add((record, n.ends, Literal(time[1], datatype=XSD.datetime)))
        add_bag_to_graph(g, record, n.affectedLines, affected_lines, XSD.string)
        add_bag_to_graph(g, record, n.classification, category, XSD.string)
        if main_category == 'Dopravní informace':
            pass
        elif main_category == 'Mimořádná událost v MHD':
            g.add((record, n.affecedStop, Literal(stop, datatype=XSD.string)))
            g.add((record, n.dirrection, Literal(direction, datatype=XSD.string)))
            g.add((record, n.reason, Literal(reason, datatype=XSD.string)))
            g.add((record, n.delay_amount, Literal(delay, datatype=XSD.string)))
            g.add((record, n.possibleSolution, Literal(sug_solution, datatype=XSD.string)))
        elif main_category == 'Výluka v MHD':
            pass
