"""
解析Markdown文件，按照层级结构进行解析返回。Markdown格式参考 https://www.markdownguide.org/basic-syntax/

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
import re

from wikidata_filter.loader.text import TextBase

header_pattern = re.compile(r"^(#+)\s*(.*)")
table_pattern = re.compile(
    r'(\|(?:[^|\n]+\|)+)\s*\n'  # 匹配表头，忽略行末空格
    r'(\|(?:[-: ]+\|)+)\s*\n'   # 匹配分隔行
    r'((?:\|(?:[^|\n]+\|)+\s*\n)*)',  # 匹配数据行
    re.DOTALL
)


class Markdown(TextBase):
    """
    解析处理markdown数据，处理成树状结构返回
    """
    def __init__(self,
                 input_file: str,
                 encoding: str = "utf8",
                 tables: str = None,
                 **kwargs):
        """
        :param input_file markdown文件路径
        :param encoding 文件编码
        :param tables 提取表格的标志，默认None表示不提取；否则表示返回格式：raw、json
        """
        super().__init__(input_file, encoding=encoding, **kwargs)
        self.tables = tables

    def iter(self):
        tree = self.parse_markdown_to_tree()
        if self.tables:
            self.extract_tables(tree)
        yield tree

    def parse_markdown_to_tree(self):
        """
        将Markdown内容解析为树状结构。
        """
        root = {"level": 0}
        # 用于维护层级结构的栈
        stack = [root]
        for line in self.instream:
            line = line.strip()
            # 判断是否为标题行
            header_match = header_pattern.match(line)
            if header_match:
                # 标题级别（#的数量）
                level = len(header_match.group(1))
                title = header_match.group(2).strip()
                node = {"title": title, "level": level}
                # 找到父节点
                while stack[-1]["level"] >= level:
                    stack.pop()
                parent = stack[-1]
                if "children" in parent:
                    parent["children"].append(node)
                else:
                    parent["children"] = [node]
                stack.append(node)
            else:
                # 如果不是标题行，则将其作为内容
                parent = stack[-1]
                if parent.get('content'):
                    parent['content'] += "\n" + line
                else:
                    parent['content'] = line
        return root

    def extract_tables(self, node: dict, keep_table_content: bool = False):
        """
        提取节点中的表格，并将其解析为指定的格式。
        """
        content = node.get("content")
        if content:
            tables = self.parse_table(content)
            if tables:
                if self.tables == "json":
                    tables = [self.as_json_rows(table) for table in tables]
                node["tables"] = tables
            if tables and not keep_table_content:
                node["content"] = re.sub(table_pattern, "", content).strip()
        # 递归处理子节点
        for child in node.get("children", []):
            self.extract_tables(child)

    @staticmethod
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

    @staticmethod
    def as_json_rows(table: dict):
        header = table["header"]
        rows = table["rows"]
        return [
            {k: val for k, val in zip(header, row)}
            for row in rows if len(row) == len(header)
        ]
