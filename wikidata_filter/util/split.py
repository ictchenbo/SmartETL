def chinese_simple(content: str, *args, **kwargs):
    """"中文拆分"""
    from wikidata_filter.integrations.chinese_text_splitter import CharacterTextSplitter
    util = CharacterTextSplitter()
    return util.split_text(content)


def simple(content: str, max_length: int = 100, **kwargs):
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
