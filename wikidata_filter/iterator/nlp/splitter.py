import re
from wikidata_filter.iterator.base import DictProcessorBase
from wikidata_filter.iterator.mapper import Map


def split_chinese_simple(content: str):
    """"中文拆分"""
    from wikidata_filter.integrations.chinese_text_splitter import CharacterTextSplitter
    util = CharacterTextSplitter()
    return util.split_text(content)


def split_simple(content: str, max_length: int = 100):
    """简单拆分算法"""
    res = []
    while len(content) > max_length:
        pos = max_length - 1
        while pos < len(content) and content[pos] not in "。？！；\n":
            pos += 1
        if pos < len(content):
            res.append(content[:pos+1])
            content = content[pos+1:]
        else:
            break
    if content:
        res.append(content)

    return res


class TextSplit(Map):
    """文本拆分"""
    def __init__(self, key: str = None, target_key: str = None, algorithm: str = "simple"):
        super().__init__(self, key=key, target_key=target_key)
        self.algorithm = algorithm or "simple"
        g = globals()
        func_name = f'split_{self.algorithm}'
        if func_name in g:
            self.func = g[func_name]
        else:
            raise ImportError(f'No such function: {func_name}')

    def __call__(self, text, *args, **kwargs):
        if isinstance(text, list):
            text = '\n'.join(text)
        return self.func(text)


def split(text: str):
    if not text:
        return []
    if ':' in text:
        text = text[text.find(':') + 1:]
    if '：' in text:
        text = text[text.find('：') + 1:]
    parts = re.split('[;；。]+', text)
    return [p.strip() for p in parts]


class TagSplit(DictProcessorBase):
    """标签拆分"""
    def __init__(self, *keys):
        self.keys = keys

    def on_data(self, data: dict, *args):
        for key in self.keys:
            if key in data:
                data[key] = split(data.get(key))
        return data
