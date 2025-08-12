"""论文解析相关算子 根据markdown/json等格式转换为统一论文格式"""
from wikidata_filter.gestata.digest import sha256_id, pad20
from wikidata_filter.integrations.chinese_text_splitter import CharacterTextSplitter


def add_meta(row: dict):
    """构造论文数字ID及其存储路径 注意_id需要根据业务提前构造"""
    if 'filename' not in row and 'url' in row:
        row['filename'] = row['url'][row['url'].rfind('/')+1:]
    if '_id' in row:
        _id = row['_id']
        _did = sha256_id(_id.encode('utf8'), length=7)
        sid = pad20(_did, length=18)
        row['id'] = _did
        row['sid'] = sid
        store_path = '/'.join([sid[:3], sid[3:6], _id])
        row['path_html'] = store_path + '.html'
        row['path_pdf'] = store_path + '.pdf'
        row['path_md'] = store_path + '.md'
        row['store_path'] = store_path + '.json' # 主数据

    return row


def fill_paper(row: dict,
               target_key: str = 'paper',
               copy_keys: tuple = ('_id', 'id', 'store_path'),
               data: dict = None):
    """填充论文主结构的相关字段"""
    target = row[target_key]
    for key in copy_keys:
        if key in row:
            target[key] = row[key]
    if data:
        target.update(data)

    source = target.get('source') or {}
    source['filename'] = row.get('filename')
    target['source'] = source

    return row


def image_list(row: dict, key='images'):
    """生成图像列表"""
    images = row.get(key) or []
    results = []
    for i, image in enumerate(images):
        # 忽略过多的图片
        if i >= 100:
            break
        results.append({
            "_id": row["id"] * 100 + i,
            "index": i, # 图片在论文中的序号
            "paper_id": row["_id"],
            "paper_store_path": row["store_path"], #指向主数据
            "data": image,  # 作为payload或单独存储 通过store_path查找
            "store_path": row["store_path"][:-5] + f'.{i:02d}.png'
        })
    return results


def __chunks(row: dict, content: str):
    util = CharacterTextSplitter()
    chunks = util.split_text(content)
    results = []
    for i, chunk in enumerate(chunks):
        # 忽略过长的内容
        if i >= 100:
            break
        results.append({
            "_id": row["id"] * 100 + i,
            "index": i,  # chunk在论文中的序号
            "paper_id": row["_id"],
            "paper_store_path": row["store_path"], #指向主数据
            "text": chunk  # 作为payload而不必单独存储
        })

    return results


def chunks_from_paper(row: dict, key='paper'):
    """论文正文简单拆分"""
    paper = row.get(key) or {}
    texts = []
    if 'title' in paper:
        texts.append("title: " + paper['title'])
    if 'abstract' in paper:
        texts.append("abstract: " + paper['abstract'])
    texts.append('\n')
    for section in paper.get('sections') or []:
        title = section.get('title') or ''
        if title:
            texts.append('# ' + title)
        texts.append(section.get('content') or '')

    content = '\n'.join(texts)
    return __chunks(row, content)


def chunks_from_markdown(row: dict, key='md'):
    """基于论文markdown的简单拆分"""
    content = row.get(key)
    return __chunks(row, content)


def from_json(doc: dict):
    """基于已经解析的层级化json文档结构进行转换"""
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


def simple_chunk_from_paper(paper: dict, fields: list = ('title', 'abstract')):
    """基于论文结构的简单chunk"""
    lines = []
    for key in fields:
        tag = key.upper()
        lines.append(f'<{tag}>{paper.get(key)}</{tag}>')

    return '\n'.join(lines)


if __name__ == '__main__':
    import json
    json_tree = open('../../data/arxiv/mineru.resultv3.json', encoding="utf8").read()
    paper = from_json(json.loads(json_tree)["paper"])
    print(json.dumps(paper, indent=2))
