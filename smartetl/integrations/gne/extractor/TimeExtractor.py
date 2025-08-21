import re
import json
import traceback

from smartetl.integrations.gne.utils import config
from lxml.html import HtmlElement
from smartetl.integrations.gne.defaults import (DATETIME_PATTERN, PUBLISH_TIME_META,
                                                PUBLISH_TIME_META_2, PUBLISH_TIME_LD,
                                                PUBLISH_TIME_TIME, PUBLISH_TIME_OTHER,
                                                UTC_FORMAT, DATETIME_PATTERN_V2, DATETIME_PATTERN_URL)


def is_datetime(text: str):
    for dt in DATETIME_PATTERN:
        if re.search(dt, text):
            return True
    return False


class TimeExtractor:

    def extract(self, element: HtmlElement, publish_time_xpath: str = '') -> str:
        publish_time_xpath = publish_time_xpath or config.get('publish_time', {}).get('xpath')
        publish_time = (self.extract_from_user_xpath(publish_time_xpath, element)  # 用户指定的 Xpath 是第一优先级
                        or self.extract_from_meta_end(element)   # 第二优先级从 <meta> 中提取
                        or self.extract_from_ldjson(element)  # 第三优先级 JSON-LD
                        or self.extract_from(element, PUBLISH_TIME_TIME)  # 第四优先级 <time>
                        or self.extract_from(element, PUBLISH_TIME_OTHER)  # 第五优先级 其他可能的标签
                        or self.extract_from_text(element))  # 最后从文本提取
        return publish_time

    def extract_from_user_xpath(self, publish_time_xpath: str, element: HtmlElement) -> str:
        if publish_time_xpath:
            publish_time = ''.join(element.xpath(publish_time_xpath))
            return publish_time
        return ''

    def extract_from_text(self, element: HtmlElement, mode='all', keep_script=True) -> str:
        # 移除style/script节点 否则可能会误导正则匹配
        for tag in element.xpath("//style"):
            tag.getparent().remove(tag)
        if not keep_script:
            for tag in element.xpath("//script"):
                tag.getparent().remove(tag)
        xpath = './/text()' if mode == 'all' else '//body//text()' if mode == 'body' else '//head//text()'
        text = ''.join(element.xpath(xpath))
        for dt in DATETIME_PATTERN:
            dt_obj = re.search(dt, text)
            if dt_obj:
                return dt_obj.group()
        return ''

    def extract_utc_date(self, html: str) -> str:
        dt_obj = re.search(UTC_FORMAT, html)
        if dt_obj:
            return dt_obj.group()
        return ''

    def extract_from_html(self, html: str) -> str:
        for dt in DATETIME_PATTERN_V2:
            dt_obj = re.search(dt, html)
            if dt_obj:
                return dt_obj.group()
        return ''

    def extract_from_url(self, url: str) -> str:
        date_match = re.search(DATETIME_PATTERN_URL, url)
        if date_match:
            return date_match.group()
        return ''

    def extract_from(self, element: HtmlElement, xpaths: list = PUBLISH_TIME_META) -> str:
        """
        一些很规范的新闻网站，会把新闻的发布时间放在 META 中，因此应该优先检查 META 数据
        :param element: 网页源代码对应的Dom 树
        :param xpaths: xpath列表
        :return: str
        """
        for xpath in xpaths:
            publish_time = element.xpath(xpath)
            if publish_time:
                return publish_time[0]
                # return ''.join(publish_time)
        return ''

    def extract_from_meta_end(self, element: HtmlElement) -> str:
        """
        一些很规范的新闻网站，会把新闻的发布时间放在 META 中，因此应该优先检查 META 数据
        :param element: 网页源代码对应的Dom 树
        :return: str
        """
        for prop, ends in PUBLISH_TIME_META_2.items():
            tags = element.xpath(f"//meta[@{prop}]")
            if tags:
                for tag in tags:

                    prop_val = tag.get(prop)
                    for end in ends:
                        if prop_val.endswith(end):
                            return tag.get("content")
        return ''

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

    def find_value_from_ld(self, ld_row: dict, key: str):
        if ld_row.get('@type') in ['NewsArticle', 'Article', 'WebPage']:
            if key in ld_row:
                return ld_row[key]
        else:
            r = self.deep_search(ld_row, key)
            if r:
                return r
        return None

    def extract_from_ldjson(self, element: HtmlElement) -> str:
        """
        一些新闻网站会使用application/ld+json数据 其中可能包含发布时间
        :param element: 网页源代码对应的Dom树
        :return: str
        """
        scripts = element.xpath('//script[@type="application/ld+json"]/text()')
        for script in scripts:
            # 移除json中不合法的字符
            json_content = re.sub(r'[\n\r\t]+', '', script).strip()
            if not json_content:
                continue
            try:
                data = json.loads(json_content)
                if isinstance(data, list):
                    for v in data:
                        if isinstance(v, dict):
                            r = self.find_value_from_ld(v, PUBLISH_TIME_LD)
                            if r:
                                return r
                elif isinstance(data, dict):
                    r = self.find_value_from_ld(data, PUBLISH_TIME_LD)
                    if r:
                        return r
            except:
                traceback.print_exc()
                print("invalid application/ld+json", json_content)

        return ''
