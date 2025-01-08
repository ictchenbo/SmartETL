import re
import requests
from wikidata_filter.iterator.base import DictProcessorBase
from wikidata_filter.util.html import text_from_html

try:
    from gne import GeneralNewsExtractor
except:
    print("gne not installed")
    raise Exception("gne not installed")

title_pattern = '(?<=<title>)(.*?)(?=</title>)'
time_fields = ['article:published_time', 'datePublished', 'article:modified_time', 'dateModified']
KEY_DESC = ['description', 'og:description']
KEY_KEYWORDS = ['keywords', 'og:keywords']
KEY_SITES = ['og:site_name']
# KEY_AUTHOR = ['author', 'og:author']


def extract_title(html: str):
    match = re.search(title_pattern, html)
    if match:
        text = html.unescape(match.group(0))
        title_list = list(map(lambda s: s.strip(), re.split(' - | \| | – | — ', text)))
        long_title = str(max(title_list, key=len))
        long_index = title_list.index(long_title)
        if long_index == 0:
            return long_title
        pos = text.index(long_title) + len(long_title)
        return text[:pos]
    return None


def find_value(meta: dict, keys: list):
    for key in keys:
        if key in meta:
            return meta[key]
    return None


class Extract(DictProcessorBase):
    """基于gne抽取新闻的标题、正文、时间、作者、图片等信息"""

    def __init__(self, key: str = 'html', target_key: str = None):
        self.extractor = GeneralNewsExtractor()
        self.key = key
        self.target_key = target_key or key

    def on_data(self, data: dict, *args):
        html = data.get(self.key)
        if html is None:
            return {}
        news = self.extract(html)
        publish_time = None
        if news:
            if not news.get("title"):
                news["title"] = extract_title(html)
            if news.get("time"):
                publish_time = news.pop("time")
        data[self.target_key] = news
        data['meta'] = text_from_html(html, text=False, meta=True)[1]
        metas = {kv[0]: kv[1] for kv in data['meta']}
        if publish_time is None:
            publish_time = find_value(metas, time_fields)
        if publish_time:
            data['publish_time'] = publish_time

        data['keywords'] = find_value(metas, KEY_KEYWORDS)
        data['desc'] = find_value(metas, KEY_DESC)
        data['site_name'] = find_value(metas, KEY_SITES)

        return data

    def extract(self, html: str) -> dict:
        try:
            return self.extractor.extract(html)
        except:
            return {}


class Constor(Extract):
    """基于Constor组件的新网网页信息抽取"""
    def __init__(self, api_base: str, key: str = 'html', target_key: str = None, request_timeout: int = 120):
        super().__init__(key=key, target_key=target_key)
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
