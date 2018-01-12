# Data source urls
from data_pipeline import BASE_NAMESPACE

PT_PRAGUE_CHANGES_URL = 'http://www.dpp.cz/rss/doprava/'
PT_PRAGUE_IRREGULARITY_URL = 'http://www.dpp.cz/rss/mimoradne-udalosti/'
PT_BRNO_CHANGES_URL = 'http://dpmb.cz/cs/vsechna-omezeni-dopravy'
PT_PILSEN_CHANGES_URL = 'http://www.pmdp.cz/zmeny-v-doprave.xml'

# Namespace
PT_PRAGUE_CHANGES_NAME_SPACE = BASE_NAMESPACE + 'prague/changes/'
PT_PRAGUE_IRREGULARITY_NAME_SPACE = BASE_NAMESPACE + 'prague/irregularity/'
PT_BRNO_CHANGES_NAME_SPACE = BASE_NAMESPACE + 'brno/changes/'
PT_PILSEN_CHANGES_NAME_SPACE = BASE_NAMESPACE + 'pilsen/changes/'
