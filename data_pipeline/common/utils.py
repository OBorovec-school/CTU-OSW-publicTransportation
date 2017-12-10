import rdflib
from rdflib import RDF, Literal
from rdflib.namespace import FOAF


def get_element_nonroot_xml(xml_text, el_name):
    return xml_text.split('<' + str(el_name) + '>')[1].split('</' + str(el_name) + '>')[0]


########################################################################################################################
# Easy manipulation with RDF
########################################################################################################################
def add_bag_to_graph(g, el, bag_pred, items, item_type):
    bag = rdflib.BNode()
    g.add((bag, RDF.type, RDF.Bag))
    g.add((el, bag_pred, bag))
    for item in items:
        g.add((bag, RDF.li, Literal(item, datatype=item_type)))