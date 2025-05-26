"""论文解析相关算子 根据markdown/json等格式转换为统一论文格式"""


def from_json(doc: dict):
    nodes = doc.get("children") or []
    if not nodes:
        return None

    title_node = nodes[0]
    paper = {
        "title": title_node.get("title"),
        "authors": title_node.get("content")
    }
    sections = []

    for i in nodes[1:]:
        title = (i.get("title") or "").strip().upper()
        content = i.get("content") or ""
        if title.startswith("ABSTRACT"):
            paper["abstract"] = content
        elif title.startswith("KEYWORDS"):
            paper["keywords"] = content
        elif title.startswith("REFERENCES"):
            paper["references"] = content.split('\n')
        else:
            contents, tables = process_section(i)
            section = {
                "title": title,
                "content": '\n'.join(contents[1:]),
                "tables": tables
            }
            sections.append(section)
    paper['sections'] = sections

    return paper


def process_section(node: dict):
    """层级化结构转化扁平化的结构"""
    header_line = node.get('title')
    if node.get('number'):
        header_line = node.get('number') + ' ' + header_line
    # header_line = '#' * node.get('level', 1) + header_line

    content = [header_line]
    tables = []
    v = node.get('content')
    if v:
        content.append(v)

    if not node.get("children"):
        return content, tables

    for sub in node.get("children"):
        c, t = process_section(sub)
        content.extend(c)
        tables.extend(t)

    return content, tables


if __name__ == '__main__':
    import json
    json_tree = open('../../data/arxiv/mineru.resultv3.json', encoding="utf8").read()
    paper = from_json(json.loads(json_tree)["paper"])
    print(json.dumps(paper, indent=2))
