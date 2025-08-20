
try:
    from wikimarkup.parser import Parser
except:
    print("wikimarkup is not installed!")
    raise "wikimarkup is not installed!"


parser = Parser()


def to_html(markup: str):
    """基于wikipedia对象的WikiMarkup格式内容生成对应的HTML"""
    return parser.parse(markup)


def get_abstract(text: str):
    """基于wikipedia文本，提取第一段作为摘要"""
    if text:
        pos = text.find('\n\n')
        if pos > 0:
            return text[0:pos]
        else:
            return text
    return None
