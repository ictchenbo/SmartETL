"""
解析Markdown格式文本，按照层级结构进行解析返回。Markdown格式参考 https://www.markdownguide.org/basic-syntax/

示例文件：test_data/人工智能简介.md
解析结果：test_data/markdownfile.json

返回结构如下：
{
  "content": "",
  "tables": [], //如果有表格 list[Table]
  "children": [
     {
        "title": "标题1",
        "content": "正文1",
        "tables": [], //如果有表格 list[Table]
        "children": [
           {
              "title": "标题2",
              "content": "正文2
           }
        ]
     }
  ]
}

支持提取表格内容，表结构Table支持两种格式，其中raw格式如下：
{
  "header": ["col1", "col2"],
  "rows": [
     ["col11", "col12"],
     ["col21", "col22"]
  ]
}

json格式如下：
[
 {
   "col1": "col11",
   "col2": "col12"
 },
 {
   "col1": "col21",
   "col2": "col22"
 }
]

"""
import json
import re
from io import StringIO

header_pattern = re.compile(r"^(#+)\s*(.*)")
header_number_pattern = re.compile(r"^\d+\.\d+(\.\d+)*|\d\.")
table_pattern = re.compile(
    r'(\|(?:[^|\n]+\|)+)\s*\n'  # 匹配表头，忽略行末空格
    r'(\|(?:[-: ]+\|)+)\s*\n'   # 匹配分隔行
    r'((?:\|(?:[^|\n]+\|)+\s*\n)*)',  # 匹配数据行
    re.DOTALL
)


def parse_heading(level: int, title: str):
    number = header_number_pattern.match(title)
    if number:
        number = number.group(0)
        n = 0
        for vi in number.split('.'):
            if vi and vi.isdigit():
                n += 1
        return n, title[len(number):].strip(), number
    return level, title, None


def extract(content: str, tables: str = None, numbered: bool = False):
    """
    将Markdown内容解析为树状结构。
    :param content 待解析的Markdown内容
    :param tables 提取表格的标志，默认None表示不提取；否则表示返回格式：raw、json
    :param numbered 正文标题按有编号处理 如果为True 则根据编号自动调整节点位置
    """
    root = {"level": 0}
    # 用于维护层级结构的栈
    stack = [root]

    def find_parent(n: dict):
        nodes = root.get("children") or []
        if not nodes:
            return False
        num = n["number"]
        node = nodes[-1]
        while num.startswith(node.get("number") or "------") and node.get("children"):
            node = node["cihldren"][-1]
        if node.get("children"):
            node["children"].append(n)
        else:
            node["children"] = [n]
        return True

    def append_content(c: str):
        nodes = root.get("children") or []
        if not nodes:
            return False
        node = nodes[-1]
        while node.get("children"):
            node = node["cihldren"][-1]
        node["content"] += '\n' + c
        return True

    # all_nodes = [root]
    instream = StringIO(content)
    for line in instream:
        line = line.strip()
        # 判断是否为标题行
        header_match = header_pattern.match(line)
        if header_match:
            # 标题级别（#的数量）
            level = len(header_match.group(1))
            title = header_match.group(2).strip()
            level, title, number = parse_heading(level, title)
            node = {"title": title, "level": level, "number": number}
            # if number and numbered:
            #     if find_parent(node):
            #         continue
            # else:
            #     if append_content(title)
            # 找到父节点
            while stack[-1]["level"] >= level:
                stack.pop()
            parent = stack[-1]
            if "children" in parent:
                parent["children"].append(node)
            else:
                parent["children"] = [node]
            stack.append(node)
            # all_nodes.append(node)
        else:
            # 如果不是标题行，则将其作为内容
            parent = stack[-1]
            if parent.get('content'):
                parent['content'] += "\n" + line
            else:
                parent['content'] = line
    if tables:
        extract_tables(root, table_format=tables)
    return root


def extract_tables(node: dict, keep_table_content: bool = False, table_format="raw"):
    """
    提取节点中的表格，并将其解析为指定的格式。
    """
    content = node.get("content")
    if content:
        tables = parse_table(content)
        if tables:
            if table_format == "json":
                tables = [as_json_rows(table) for table in tables]
            node["tables"] = tables
        if tables and not keep_table_content:
            node["content"] = re.sub(table_pattern, "", content).strip()
    # 递归处理子节点
    for child in node.get("children", []):
        extract_tables(child)


def parse_table(content: str):
    """
    解析Markdown表格并返回结构化数据。
    """
    table_data = []
    for match in table_pattern.findall(content):
        header_row = match[0].strip()
        header = [h.strip() for h in header_row[1:-1].split("|")]
        data_rows = match[2].strip().split("\n")
        data = []
        for row in data_rows:
            cells = [c.strip() for c in row.strip()[1:-1].split("|")]
            data.append(cells)
        table_data.append({
            "header": header,
            "rows": data
        })
    return table_data


def as_json_rows(table: dict):
    header = table["header"]
    rows = table["rows"]
    return [
        {k: val for k, val in zip(header, row)}
        for row in rows if len(row) == len(header)
    ]


if __name__ == '__main__':
    # print(parse_heading(1, "1. Introduction"))
    # print(parse_heading(1, "2.1 LLM models"))

    # parent_index = len(stack) - 1
    # if number:
    #     # 如果存在序号，通过序号前缀发找到真正的上级目录
    #     while parent_index > 0 and stack[parent_index] and stack[parent_index]["number"] and not number.startswith(
    #             stack[parent_index]["number"]):
    #         parent_index -= 1
    # parent = stack[parent_index]

    md_text = open('../../data/arxiv/markdown/1209.6492v1.pdf.md', encoding="utf8").read()
    tree = extract(md_text, numbered=True)
    print(json.dumps(tree, indent=2))
