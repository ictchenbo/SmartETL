import re
import json

try:
    from bs4 import BeautifulSoup
except:
    raise ImportError("bs4 not installed")


def meta_tag(tag):
    if not tag.has_attr("content"):
        return None
    name = None
    if tag.has_attr("property"):
        name = tag["property"]
    elif tag.has_attr("name"):
        name = tag["name"]
    if name:
        return name, tag["content"]
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
        self.meta = self.get_meta()
        self.ld_data = self.get_ld_data()
        self.metas_s = simple_meta(self.meta)

    def get_text(self):
        return self.soup.get_text(separator=' ', strip=True)

    def get_meta(self):
        _meta = []
        tags = self.soup.select("html>head>meta")
        for tag in tags:
            kv = meta_tag(tag)
            if kv:
                _meta.append(kv)
        return _meta

    def get_ld_data(self):
        scripts = self.soup.find_all('script', type='application/ld+json')
        for script in scripts:
            # 提取script标签中的内容
            json_content = script.get_text().strip()
            # 将JSON字符串转换为Python字典
            try:
                data = json.loads(json_content)
                return data
            except:
                print("invalid application/ld+json", json_content)

        return None

    def find_value_by_key(self, key: str = 'date'):
        if key in self.metas_s:
            return self.metas_s[key]
        if self.ld_data:
            if key in self.ld_data:
                return self.ld_data[key]
            if '@graph' in self.ld_data:
                rows = self.ld_data['@graph']
                for row in rows:
                    if isinstance(row, dict):
                        if key in row:
                            return row[key]
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
    soup = BeautifulSoup(html, 'html.parser')

    title = soup.title.string
    content = soup.body.text.strip()
    # content = soup.get_text(separator=' ', strip=True)

    return {
        "title": title,
        "text": content
    }
