import hashlib

from bs4 import BeautifulSoup
from dateutil import parser
from rdflib import RDF, Literal
from rdflib.namespace import FOAF, XSD

from data_pipeline.common.transformer.item_rdf_transformer import ItemToRDFTransformer
from data_pipeline.common.utils import add_bag_to_graph
from data_pipeline.public_transp.Brno import PT_BRNO_CHANGES_NAME_SPACE
from data_pipeline.public_transp.Brno.web_crawler import ChangeWebCrawler


class PTBrnoChangesRDF(ItemToRDFTransformer):
    NAME = 'PTBrnoChangesRDF'
    NAMESPACE = PT_BRNO_CHANGES_NAME_SPACE
    NAMESPACE_PREFIX = 'brnoChanges'
    LUIGI_OUTPUT_FILE = 'PTBrnoChangesRDF'

    def requires(self):
        return ChangeWebCrawler(self.unique_param)

    def parse_item_to_graph(self, item, g, n):
        id = hashlib.md5(str(item).encode('utf-8')).hexdigest()
        main_info = BeautifulSoup(item[0], "lxml")
        side_info = BeautifulSoup(item[1], "lxml")

        title = main_info.find('div', {'class': 'boxTxt truncNps'}).text
        affected_lines = main_info.find('div', {'class': 'boxLink truncLnk'}).text.replace('Linky: ','').strip().split(',')
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
        g.add((record, RDF.type, FOAF.TrafficChange))
        g.add((record, FOAF.title, Literal(title, datatype=XSD.string)))
        g.add((record, FOAF.starts, Literal(time[0], datatype=XSD.datetime)))
        g.add((record, FOAF.ends, Literal(time[1], datatype=XSD.datetime)))
        add_bag_to_graph(g, record, FOAF.affectedLines, affected_lines, XSD.string)
        add_bag_to_graph(g, record, FOAF.classification, category, XSD.string)
        if main_category == 'Dopravní informace':
            pass
        elif main_category == 'Mimořádná událost v MHD':
            g.add((record, FOAF.affecedStop, Literal(stop, datatype=XSD.string)))
            g.add((record, FOAF.dirrection, Literal(direction, datatype=XSD.string)))
            g.add((record, FOAF.reason, Literal(reason, datatype=XSD.string)))
            g.add((record, FOAF.delay_amount, Literal(delay, datatype=XSD.string)))
            g.add((record, FOAF.possibleSolution, Literal(sug_solution, datatype=XSD.string)))
        elif main_category == 'Výluka v MHD':
            pass
