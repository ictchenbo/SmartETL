import requests
from wikidata_filter.integrations.gne import GeneralNewsExtractor
from wikidata_filter.iterator.mapper import Map
from wikidata_filter.util.extractor.html import news_merge


class Extract(Map):
    """基于gne抽取新闻的标题、正文、时间、作者、图片等信息"""
    extractor = GeneralNewsExtractor()

    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)

    def __call__(self, html: str, *args, **kwargs):
        if html is None or len(html) < 100:
            print("Warning: html is None")
            return {}
        return news_merge(html, self.extract(html))

    def extract(self, html: str) -> dict:
        try:
            return self.extractor.extract(html)
        except:
            return {}


class Constor(Extract):
    """基于Constor组件的新网网页信息抽取"""
    def __init__(self, api_base: str, key: str = 'html', request_timeout: int = 120, **kwargs):
        super().__init__(key=key, **kwargs)
        self.api_base = api_base
        self.request_timeout = request_timeout

    def extract(self, html: str) -> dict:
        for i in range(3):
            try:
                r = requests.post(self.api_base, json=[html], timeout=self.request_timeout)
                if r.status_code == 200:
                    return r.json()[0]
                else:
                    print(f'error response from gdelt parser. {r.status_code}:{r.text}')
            except:
                print("Constor服务异常")

        return super().extract(html)
