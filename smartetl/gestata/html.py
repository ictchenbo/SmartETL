import re

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


def simple(html: str, is_snippet: bool = False):
    """从HTML中提取标题和正文（简单方法）"""
    if is_snippet:
        html = f'<html><body>{html}</body></html>'
    soup = BeautifulSoup(html, 'html.parser')

    title = soup.title.string if soup.title else None
    content = soup.body.text.strip()
    # content = soup.get_text(separator=' ', strip=True)

    return {
        "title": title,
        "content": content
    }
