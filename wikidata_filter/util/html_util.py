import re
import html

try:
    from bs4 import BeautifulSoup
except:
    raise ImportError("bs4 not installed")


title_pattern = '(?<=<title>)(.*?)(?=</title>)'


def extract_title(page_html: str):
    match = re.search(title_pattern, page_html)
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


def text_from_html(source: str, text=True, meta=False):
    assert text or meta, "at least one of text & meta should be set True"
    soup = BeautifulSoup(source, 'html.parser')
    _text, _meta = None, []
    if text:
        _text = soup.get_text(separator=' ', strip=True)
    if meta:
        tags = soup.select("html>head>meta")
        for tag in tags:
            kv = meta_tag(tag)
            if kv:
                _meta.append(kv)

    return _text, _meta
