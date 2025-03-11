import re
import json
import traceback
from wikidata_filter.util.dates import normalize_time

try:
    from gne import GeneralNewsExtractor, TimeExtractor
    from gne.utils import html2element
except:
    print("gne not installed")
    raise Exception("gne not installed")

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


date_keys = ['published_time', 'datePublished', 'publishdate']


class HtmlExtractor:
    def __init__(self, source: str):
        self.soup = BeautifulSoup(source, 'html.parser')
        self.meta = []
        self.ld_data = []
        self.metas_s = {}

    def parse_meta(self):
        self.meta = self.get_meta()
        self.metas_s = simple_meta(self.meta)

    def parse_ld(self):
        self.ld_data = self.get_ld_data()

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

    def get_ld_data(self):
        all_ld = []
        scripts = self.soup.find_all('script', type='application/ld+json')
        for script in scripts:
            # 提取script标签中的内容
            json_content = script.get_text().strip()
            # 移除json中不合法的字符
            json_content = re.sub(r'[\n\r\t]+', '', json_content).strip()
            if not json_content:
                continue
            try:
                data = json.loads(json_content)
                if isinstance(data, list):
                    all_ld.extend([v for v in data if isinstance(v, dict)])
                elif isinstance(data, dict):
                    all_ld.append(data)
            except:
                traceback.print_exc()
                print("invalid application/ld+json", json_content)

        return all_ld

    def find_value_from_meta(self, key: str):
        if key in self.metas_s:
            return self.metas_s[key]
        return None

    def deep_search(self, row: dict, key: str):
        if key in row:
            return row[key]
        for val in row.values():
            if isinstance(val, dict):
                r = self.deep_search(val, key)
                if r:
                    return r
            elif isinstance(val, list):
                for v in val:
                    if isinstance(v, dict):
                        r = self.deep_search(v, key)
                        if r:
                            return r
        return None

    def find_value_from_ld(self, key: str):
        if not self.ld_data:
            return None
        for row in self.ld_data:
            if row.get('@type') in ['NewsArticle', 'Article', 'WebPage']:
                if key in row:
                    return row[key]
            else:
                r = self.deep_search(row, key)
                if r:
                    return r
        return None
        # if key in self.ld_data:
        #     return self.ld_data[key]
        # if '@graph' in self.ld_data:
        #     rows = self.ld_data['@graph']
        #     for row in rows:
        #         if isinstance(row, dict):
        #             if key in row:
        #                 return row[key]
        # return None

    def find_value_by_key(self, key: str = 'date'):
        return self.find_value_from_ld(key) or self.find_value_from_meta(key)

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


def news(html: str):
    """基于新闻网页HTML提取基本信息"""
    doc = {}
    my_extractor = HtmlExtractor(html)
    doc['meta'] = my_extractor.meta

    if "title" not in doc:
        doc["title"] = my_extractor.find_value_by_key("title") or my_extractor.get_title()
    doc['keywords'] = my_extractor.find_value_by_key("keywords")
    doc['desc'] = my_extractor.find_value_by_key("description")
    doc['source'] = doc.get('source') or my_extractor.find_value_by_key('site_name')
    doc['author'] = doc.get('author') or my_extractor.find_value_by_key('author')

    # 发布时间处理
    publish_time = (my_extractor.find_value_from_ld('datePublished')
                    or my_extractor.find_value_from_meta('published_time')
                    or my_extractor.find_value_from_meta('datePublished'))
    if publish_time:
        doc['origin_publish_time'] = publish_time
    #     # TODO 对于非ISO格式的时间 如何判断时区？这里假设为UTC
    #     # 1基于<meta>
    #     tz = my_extractor.find_value(["timezone"])
    #     # 2 基于网站域名、服务器所在国家/地区
    #     # 3 基于网页内容主要地点
    #     doc['publish_time'] = normalize_time(publish_time, tz=tz)

    return doc


def news_gne(html: str):
    """基于gne的新闻抽取"""
    extractor = GeneralNewsExtractor()
    return extractor.extract(html)


def gne_meta_time(element):
    return TimeExtractor().extract_from_meta(element)


def gne_text_time(element):
    return TimeExtractor().extract_from_text(element)


def news_merge(html: str, doc: dict):
    my_extractor = HtmlExtractor(html)
    my_extractor.parse_meta()

    news = {}
    news['meta'] = my_extractor.get_meta()
    news["title"] = my_extractor.find_value_from_meta("title") or my_extractor.get_title()
    news['keywords'] = my_extractor.find_value_from_meta("keywords")
    news['desc'] = my_extractor.find_value_from_meta("description")
    news['source'] = my_extractor.find_value_from_meta('site_name') or my_extractor.find_value_from_meta('source')
    news['author'] = my_extractor.find_value_from_meta('author')

    for key in ["title", "source", "author", "keywords"]:
        if not news.get(key) and key in doc:
            news[key] = doc[key]

    news["content"] = doc.get("content")

    element = html2element(html)
    pt = gne_meta_time(element)
    if not pt:
        my_extractor.parse_ld()
        pt = my_extractor.find_value_from_ld('datePublished')
    if not pt:
        pt = gne_text_time(element)

    if pt:
        # TODO 对于非ISO格式的时间 如何判断时区？这里假设为UTC
        news['origin_publish_time'] = pt
        news['publish_time'] = normalize_time(pt)

    return news
