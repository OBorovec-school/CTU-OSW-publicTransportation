from bs4 import BeautifulSoup

from data_pipeline.common.producer.web_page_producer import WebPageProducer


class ChangeWebCrawler(WebPageProducer):

    NAME = 'ChangeWebCrawler'
    WEB_URL = 'http://dpmb.cz/cs/vsechna-omezeni-dopravy'
    META_FILE = 'ChangeWebCrawler'
    LUIGI_OUTPUT_FILE = 'ChangeWebCrawler'

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