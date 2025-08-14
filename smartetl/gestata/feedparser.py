
def from_rss(url: str):
    import feedparser as fp
    feed = fp.parse(url)

    for entry in feed.entries:
        yield {
            'title': entry.title,
            'link': entry.link,
            'published': entry.published,
            'summary': entry.summary
        }


def from_xml(url: str):
    import requests
    import xmltodict
    res = requests.get(url)
    # 注意，xmltodict.parse()非流式，仅适合小文件
    doc = xmltodict.parse(res.text)
    print(doc)
    feed = doc.get("feed") or doc.get("channel")
    for item in feed.get("item"):
        yield item
