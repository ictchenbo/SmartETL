import json

from wikidata_filter.loader.text import TextBase, Text


class WikidataJsonDump(Text):
    """
    Wikidata全量数据，Json格式，是一个非常大的Json Array，第一行为[，最后一行为]，中间每行为一个Json，行末带逗号
    尽管理论上可以直接用json.load，但并不推荐！
    """
    def __init__(self, input_file: str):
        super().__init__(input_file)

    def iter(self):
        for line in super().iter():
            if len(line) > 4:
                yield json.loads(line[:-2])


ns_prefix = 'http://www.mediawiki.org/xml/export-0.11/'
tag_prefix = '{' + ns_prefix + '}'
ns = {'wiki': ns_prefix}


def stag(t):
    return t[len(tag_prefix):]


def to_dict(elem, target: dict):
    for e in elem.findall('./*'):
        etag = stag(e.tag)
        if etag in ['comment', 'contributor']:
            continue
        if etag == 'revision':
            rev_obj = {}
            to_dict(e, rev_obj)
            target['revision'] = rev_obj
        else:
            target[etag] = e.text


class WikidataXmlIncr(TextBase):
    """
    Wikidata增量数据，仅提供XML格式，<page></page>表示一个最近修改的实体，page/revision/text为对应的Json
    """
    def __init__(self, input_file: str):
        super().__init__(input_file)

    def iter(self):
        import lxml.etree as ET
        for event, elem in ET.iterparse(self.instream, tag=f'{tag_prefix}page'):
            res = {}
            to_dict(elem, res)
            text = res['revision']['text']
            try:
                rev = json.loads(text)
            except:
                continue
            yield rev
