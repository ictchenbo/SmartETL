from smartetl.iterator.base import DictProcessorBase

try:
    from wikimarkup.parser import Parser
except:
    print("wikimarkup is not installed!")
    raise "wikimarkup is not installed!"


parser = Parser()


class ToHTML(DictProcessorBase):
    """
    基于wikipedia对象的WikiMarkup格式内容生成对应的HTML
    """
    def __init__(self, target_key: str = "html", source_key: str = 'content'):
        super().__init__()
        self.target_key = target_key
        self.source_key = source_key

    def on_data(self, item: dict, *args):
        if self.source_key in item and isinstance(item[self.source_key], str):
            content = item.get(self.source_key)
            item[self.target_key] = parser.parse(content)
        return item


class Abstract(DictProcessorBase):
    """
    基于wikipedia文本，提取第一段作为摘要
    """
    def __init__(self, target_key: str = "abstract", source_key: str = 'text'):
        super().__init__()
        self.target_key = target_key
        self.source_key = source_key

    def on_data(self, item: dict, *args):
        if self.source_key in item and isinstance(item[self.source_key], str):
            text = item[self.source_key]
            if text:
                pos = text.find('\n\n')
                if pos > 0:
                    item[self.target_key] = text[0:pos]
        return item
