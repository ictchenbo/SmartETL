from wikidata_filter.iterator.mapper import Map


try:
    from bs4 import BeautifulSoup
except:
    print("Error! you need to install bs4 first: pip install bs4")
    raise ImportError("bs4 not installed")


class Text(Map):
    """抽取HTML的基本信息：标题和文本内容"""
    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)

    def __call__(self, html: str, *args, **kwargs):
        soup = BeautifulSoup(html, 'html.parser')

        title = soup.title.string
        content = soup.body.text.strip()

        return {
            "title": title,
            "text": content
        }
