import json
from wikidata_filter.util.http import text


def E(url: str, **kwargs):
    """获取jsonp URL数据，加载JSON内容"""
    c = text(url, **kwargs)
    yield json.loads(c[c.find('(') + 1:-1])
