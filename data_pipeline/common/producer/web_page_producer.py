import urllib

from data_pipeline.common.producer.norepeat_producer import NoRepeatProducer


class WebPageProducer(NoRepeatProducer):
    NAME = NotImplementedError
    WEB_URL = NotImplementedError
    META_FILE = NotImplementedError
    LUIGI_OUTPUT_FILE = NotImplementedError

    def get_full_content(self):
        fp = urllib.request.urlopen(self.WEB_URL)
        content = fp.read().decode("utf8")
        fp.close()
        return content

    def get_entries(self, html_content):
        raise NotImplementedError
