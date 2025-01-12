import json
from wikidata_filter.loader.base import DataProvider
from wikidata_filter.util.http import text


class Jsonp(DataProvider):
    """获取jsonp URL数据，加载JSON内容"""
    def __init__(self, url: str, **kwargs):
        self.url = url
        self.kwargs = kwargs

    def iter(self):
        c = text(self.url, **self.kwargs)
        yield json.loads(c[c.find('(')+1:-1], **self.kwargs)
