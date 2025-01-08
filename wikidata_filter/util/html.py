
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


def text_from_html(html: str, text=True, meta=False):
    assert text or meta, "at least one of text & meta should be set True"
    soup = BeautifulSoup(html, 'html.parser')
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
