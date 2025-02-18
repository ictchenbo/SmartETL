from wikidata_filter.iterator.base import DictProcessorBase
from wikidata_filter.util.html_util import extract_news_article, extract_images


class Image(DictProcessorBase):
    """从新闻网页中提取图片相关信息"""
    def __init__(self, url_key: str = 'url', html_key: str = 'html'):
        self.url_key = url_key
        self.html_key = html_key

    def on_data(self, data: dict, *args, **kwargs):
        url = data[self.url_key]
        html = data[self.html_key]
        article = extract_news_article(html)
        if article:
            for img in extract_images(url, article):
                yield img
