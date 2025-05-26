import re
from wikidata_filter.util.dates import normalize_time

try:
    from bs4 import BeautifulSoup
except:
    raise ImportError("bs4 not installed")


def meta_tag(tag):
    if not tag.has_attr("content"):
        return None
    for attr in ["name", "property", "itemprop"]:
        if tag.has_attr(attr):
            return tag[attr], tag["content"]

    return None


def simple_meta(meta: list):
    res = {}
    for k, v in meta:
        if ':' in k:
            k = k[k.rfind(':')+1:]
        res[k] = v
    return res


class HtmlExtractor:
    def __init__(self, source: str):
        self.soup = BeautifulSoup(source, 'html.parser')
        self.meta = []
        self.ld_data = []
        self.metas_s = {}

    def parse_meta(self):
        self.meta = self.get_meta()
        self.metas_s = simple_meta(self.meta)

    def get_text(self):
        return self.soup.get_text(separator=' ', strip=True)

    def get_meta(self):
        _meta = []
        tags = self.soup.select("meta")
        for tag in tags:
            kv = meta_tag(tag)
            if kv:
                _meta.append(kv)
        return _meta

    def find_value_from_meta(self, key: str):
        if key in self.metas_s:
            return self.metas_s[key]
        return None

    def find_value(self, keys: list):
        for key in keys:
            if key in self.metas_s:
                return self.metas_s[key]
        return None

    def get_title(self):
        """基于正则表达式提取HTML的标题"""
        title = self.soup.find('title')
        # match = re.search(title_pattern, html_source)
        if title:
            # text = html.unescape(match.group(0))
            text = title.text.strip()
            title_list = list(map(lambda s: s.strip(), re.split(' - | \| | – | — ', text)))
            long_title = str(max(title_list, key=len))
            long_index = title_list.index(long_title)
            if long_index == 0:
                return long_title
            pos = text.index(long_title) + len(long_title)
            return text[:pos]
        return None


def simple(html: str):
    """从HTML中提取标题和正文（简单方法）"""
    soup = BeautifulSoup(html, 'html.parser')

    title = soup.title.string
    content = soup.body.text.strip()
    # content = soup.get_text(separator=' ', strip=True)

    return {
        "title": title,
        "text": content
    }


def news_merge(html: str, doc: dict = None):
    """从HTML中提取元数据 并与doc进行合并后返回。注意：时间提取的顺序"""
    doc = doc or {}
    my_extractor = HtmlExtractor(html)
    my_extractor.parse_meta()

    news = {}
    news['meta'] = my_extractor.meta
    news['keywords'] = my_extractor.find_value_from_meta("keywords")
    news['desc'] = my_extractor.find_value_from_meta("description")
    news['source'] = my_extractor.find_value_from_meta('site_name') or my_extractor.find_value_from_meta('source')
    news['author'] = my_extractor.find_value_from_meta('author')

    for key in ["source", "author", "keywords"]:
        if not news.get(key) and key in doc:
            news[key] = doc[key]

    title = my_extractor.find_value_from_meta("title") or doc.get("title") or my_extractor.get_title()
    if '|' in title:
        title = title[:title.find('|')].strip()
    news["title"] = title
    news["content"] = doc.get("content")

    pt = doc.get("publish_time") or doc.get("time")
    if pt:
        # TODO 对于非ISO格式的时间 如何判断时区？这里假设为UTC
        news['origin_publish_time'] = pt
        news['publish_time'] = normalize_time(pt)

    return news
