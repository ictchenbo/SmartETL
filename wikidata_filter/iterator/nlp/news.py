import requests

from wikidata_filter.iterator.mapper import Map

from wikidata_filter.util.extractor.html import HtmlExtractor
from wikidata_filter.util.dates import normalize_time

try:
    from gne import GeneralNewsExtractor
except:
    print("gne not installed")
    raise Exception("gne not installed")


time_fields = ['datePublished', 'published_time', 'modified_time', 'dateModified']


class Extract(Map):
    """基于gne抽取新闻的标题、正文、时间、作者、图片等信息"""

    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)
        self.extractor = GeneralNewsExtractor()

    def __call__(self, html: str, *args, **kwargs):
        if html is None or len(html) < 100:
            print("Warning: html is None")
            return {}

        news = self.extract(html)

        my_extractor = HtmlExtractor(html)

        news['meta'] = my_extractor.meta

        if "title" not in news:
            news["title"] = my_extractor.find_value_by_key("title") or my_extractor.get_title()
        news['keywords'] = my_extractor.find_value_by_key("keywords")
        news['desc'] = my_extractor.find_value_by_key("description")
        news['source'] = news.get('source') or my_extractor.find_value_by_key('site_name')
        news['author'] = news.get('author') or my_extractor.find_value_by_key('author')
        # news['link'] = news.get('url') or find_value_by_key(metas_s, 'link') or find_value_by_key(metas_s, 'url')

        # 发布时间处理
        publish_time = my_extractor.find_value_by_key('datePublished') or my_extractor.find_value_by_key('published_time')
        if not publish_time:
            if news.get("time"):
                publish_time = news.pop("time")
            elif news.get("publish_time"):
                publish_time = news.pop("publish_time")
        if publish_time:
            news['origin_publish_time'] = publish_time
            # TODO 对于非ISO格式的时间 如何判断时区？这里假设为UTC
            # 1基于<meta>
            tz = my_extractor.find_value(["timezone"])
            # 2 基于网站域名、服务器所在国家/地区
            # 3 基于网页内容主要地点
            news['publish_time'] = normalize_time(publish_time, tz=tz)

        return news

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
