from wikidata_filter.loader.base import DataProvider

from wikidata_filter.util.http import json


class HttpBase(DataProvider):
    """HTTP数据加载基类 默认请求方法为GET"""
    def __init__(self, url: str, method: str = 'get', **kwargs):
        self.url = url
        self.method = method
        self.kwargs = kwargs

    def iter(self):
        c = json(self.url, method=self.method, **self.kwargs)
        yield c

    def __str__(self):
        return f'{self.name}(url={self.url}, method={self.method})'


class Get(HttpBase):
    """GET方法请求URL，结果作为JSON"""
    def __init__(self, url: str, **kwargs):
        super().__init__(url, method='get', **kwargs)
        self.url = url


class Post(HttpBase):
    """POST方法请求URL，结果作为JSON"""
    def __init__(self, url: str, **kwargs):
        super().__init__(url, method='post', **kwargs)
        self.url = url
