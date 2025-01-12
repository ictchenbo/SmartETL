import requests
from wikidata_filter.iterator.mapper import Map
from wikidata_filter.util.html_util import text_from_html, extract_title
from wikidata_filter.util.dates import normalize_time

try:
    from gne import GeneralNewsExtractor
except:
    print("gne not installed")
    raise Exception("gne not installed")


time_fields = ['datePublished', 'article:published_time', 'article:modified_time', 'dateModified']
KEY_DESC = ['description', 'og:description']
KEY_KEYWORDS = ['keywords', 'og:keywords']


def find_value(meta: dict, keys: list):
    for key in keys:
        if key in meta:
            return meta[key]
    return None


def find_value_by_key(metas: dict, key: str = 'date'):
    for k, v in metas.items():
        if key in k.lower():
            return v
    return None


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

        news['meta'] = text_from_html(html, text=False, meta=True)[1]
        metas = {kv[0]: kv[1] for kv in news['meta']}

        if "title" not in news:
            news["title"] = find_value_by_key(metas, "title") or extract_title(html)
        news['keywords'] = find_value(metas, KEY_KEYWORDS)
        news['desc'] = find_value(metas, KEY_DESC)
        news['source'] = news.get('source') or find_value_by_key(metas, 'site_name')
        news['author'] = news.get('author') or find_value_by_key(metas, 'author')
        news['link'] = news.get('url') or find_value_by_key(metas, 'link') or find_value_by_key(metas, 'url')

        # 发布时间处理
        publish_time = None
        if news.get("time"):
            publish_time = news.pop("time")
        elif news.get("publish_time"):
            publish_time = news.pop("publish_time")
        publish_time = publish_time or find_value(metas, time_fields) or find_value_by_key(metas)
        if publish_time:
            news['origin_publish_time'] = publish_time
            # TODO 对于非ISO格式的时间 如何判断时区？这里假设为UTC
            # 1基于<meta>
            tz = find_value(metas, ["timezone"])
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


if __name__ == "__main__":
    content = open('../../../test_data/html/1219285670.html', encoding='utf8').read()

    ex = Extract()
    print(ex.extract(content))

    # ex = Constor('http://10.60.1.145:7100/constor/process')
    # print(ex(content))

