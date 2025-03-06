from wikidata_filter.loader import TextBase


class Markdown(TextBase):
    """
    解析处理markdown数据，处理成树状结构返回
    """
    def iter(self):
        data = self.instream.read()
        tree = self.parse_markdown_to_tree(data)
        if isinstance(tree,str):
            #markdown中只有纯文本文件直接返回，
            yield {"content": tree}
        else:
            # 提取表格
            for node in tree:
                self.extract_tables(node)
            yield tree

    def parse_markdown_to_tree(self, markdown_content):
        """
        将Markdown内容解析为树状结构。
        """
        import re
        # 初始化根节点
        root = {"title": "root", "level": 0, "children": []}
        # 用于维护层级结构的栈
        stack = [root]
        # 正则表达式匹配标题行
        header_pattern = re.compile(r"^(#+)\s*(.*)")
        lines = markdown_content.split("\n")
        for line in lines:
            line = line.strip()
            print(line)
            # 判断是否为标题行
            header_match = header_pattern.match(line)
            if header_match:
                # 标题级别（#的数量）
                level = len(header_match.group(1))
                # 标题内容
                title = header_match.group(2).strip()
                # 创建当前标题节点
                node = {"title": title, "level": level, "content": "", "children": []}
                # 找到父节点
                while stack[-1]["level"] >= level:
                    stack.pop()
                stack[-1]["children"].append(node)
                stack.append(node)
            else:

                # 如果不是标题行，则将其作为内容
                if stack[-1].get("content"):
                    stack[-1]["content"] += "\n" + line
                else:
                    stack[-1]["content"] = line
        return root["children"]  if root.get('children') else root['content']

    def extract_tables(self, node):
        """
        提取节点中的表格，并将其解析为指定的格式。
        """
        import re
        content = node.get("content", "")
        # 解析表格并更新节点
        node["tables"] = self.parse_table(content)
        # 移除表格后的内容
        if node["tables"]:
            # 移除表格内容
            table_pattern = r"(\|.*\|)\n(\|[-:]+[-|:\s]*\|)\n((?:\|.*\|\n?)+)"
            node["content"] = re.sub(table_pattern, "", content).strip()
        else:
            node["content"] = content.strip()
        # 递归处理子节点
        for child in node.get("children", []):
            self.extract_tables(child)

    def parse_table(self,content):
        """
        解析Markdown表格并返回结构化数据。
        """
        import re
        tables = []
        # 匹配表格的正则表达式
        table_matches = re.findall(
            r"\|(.+)\|\n\|([-:\s|]+)\|\n((?:\|.*\|\n?)+)",
            content, re.MULTILINE
        )
        for match in table_matches:
            header_row = match[0].strip()
            header = [h.strip() for h in header_row.split("|") if h.strip()]
            data_rows = match[2].strip().split("\n")
            data = []
            for row in data_rows:
                cells = [c.strip() for c in row.split("|") if c.strip()]
                if cells:
                    # 确保表头和单元格数量一致
                    if len(header) == len(cells):
                        data.append(dict(zip(header, cells)))
            tables.append(data)
        return tables