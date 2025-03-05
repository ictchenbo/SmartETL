from copy import deepcopy
from typing import Any
from wikidata_filter.util.dates import current_ts
import uuid


class Message:
    """对消息进行封装，以便在更高一层处理"""
    def __init__(self, msg_type: str, data=None):
        self.msg_type = msg_type
        self.data = data

    @staticmethod
    def end():
        return Message(msg_type="end")

    @staticmethod
    def normal(data: Any):
        return Message(msg_type="normal", data=data)


class JsonIterator:
    """流程处理算子（不包括数据加载）的基础接口"""
    def _set(self, **kwargs):
        """设置组件参数，提供对象属性链式设置"""
        for k, w in kwargs.items():
            setattr(self, k, w)
        return self

    def _get(self, key: str):
        """获取组件参数"""
        return getattr(self, key)

    def on_start(self):
        """处理数据前，主要用于一些数据处理的准备工作，不应该用于具体数据处理"""
        pass

    def on_data(self, data: Any, *args):
        """处理数据的方法。根据实际需要重写此方法。"""
        pass

    def __process__(self, data: Any, *args):
        """内部调用的处理方法，先判断是否为None 否则调用on_data进行处理，普通节点的on_data方法不会接收到None"""
        # print(f'{self.name}.__process__', data)
        if data is not None:
            if isinstance(data, Message):
                if data.msg_type == 'end':
                    # print(f'{self.name} end')
                    pass
                else:
                    self.on_data(data.data)
            else:
                return self.on_data(data)

    def on_complete(self):
        """结束处理。主要用于数据处理结束后的清理工作，不应该用于具体数据处理"""
        pass

    @property
    def name(self):
        return self.__class__.__name__

    def __str__(self):
        return f"{self.name}"


class ToDict(JsonIterator):
    """数据转换为字典"""
    def __init__(self, key: str = 'd'):
        self.key = key

    def on_data(self, data: Any, *args):
        return {self.key: data}


class ToArray(JsonIterator):
    """数据转换为数组"""
    def on_data(self, data: Any, *args):
        return [data]


class DictProcessorBase(JsonIterator):
    """针对dict类型数据处理的基类 如果传入的非字典将不做任何处理"""
    def __process__(self, data: Any, *args):
        if data is not None:
            if isinstance(data, dict):
                return self.on_data(data)
            print('Warning: data is not a dict')
            return data


class Repeat(JsonIterator):
    """重复发送某个数据多次（简单循环）"""
    def __init__(self, num_of_repeats: int):
        super().__init__()
        self.num_of_repeats = num_of_repeats

    def on_data(self, data, *args):
        for i in range(self.num_of_repeats):
            yield data

    def __str__(self):
        return f'{self.name}[num_of_repeats={self.num_of_repeats}]'


class Prompt(JsonIterator):
    """打印提示信息"""
    def __init__(self, msg: str):
        self.msg = msg

    def on_data(self, data, *args):
        print(self.msg)
        return data


class Print(JsonIterator):
    """
    打印数据，方便查看中间结果
    """
    def __init__(self, *keys, with_id: bool = False):
        if keys and isinstance(keys[0], list):
            self.keys = keys[0]
        else:
            self.keys = keys
        self.with_id = with_id

    def on_data(self, data, *args):
        _data = data
        if self.keys and isinstance(data, dict):
            _data = {k: data[k] for k in self.keys if k in data}
        if self.with_id:
            print(id(data), _data)
        else:
            print(_data)
        return data


class MarkdowToJsonCommon(JsonIterator):
    """
    解析处理markdown数据，处理成树状结构返回
    """
    def on_data(self, data, *args):
        # 解析Markdown内容为树状结构
        tree = self.parse_markdown_to_tree(data)
        # 提取表格
        for node in tree:
            self.extract_tables(node)
        return tree

    def parse_markdown_to_tree(self,markdown_content):
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
        # 逐行解析
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
                # 将当前节点添加到父节点的 children 中
                stack[-1]["children"].append(node)
                # 将当前节点压入栈
                stack.append(node)
            else:
                # 如果不是标题行，则将其作为内容
                if stack[-1]["content"]:
                    stack[-1]["content"] += "\n" + line
                else:
                    stack[-1]["content"] = line
        return root["children"]  # 返回根节点的子节点

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


class Count(JsonIterator):
    """
    计数节点 对流经的数据进行计数 并按照一定间隔进行打印输出
    """
    def __init__(self, ticks=1000, label: str = '-'):
        super().__init__()
        self.counter = 0
        self.ticks = ticks
        self.label = label

    def on_data(self, item, *args):
        self.counter += 1
        if self.counter % self.ticks == 0:
            print(f'Counter[{self.label}]:', self.counter)
        return item

    def on_complete(self):
        print(f'Counter[{self.label}] finish, total:', self.counter)

    def __str__(self):
        return f"{self.name}(ticks={self.ticks},label='{self.label}')"


class AddTS(DictProcessorBase):
    """添加时间戳"""
    def __init__(self, key: str, millis: bool = True, upsert: bool = False):
        """
        :param key 时间戳字段
        :param millis 是否为毫秒（默认） 否则为妙
        :param upsert 是否为upsert模式（默认为False）
        """
        self.key = key
        self.millis = millis
        self.upsert = upsert

    def on_data(self, data: dict, *args):
        if self.upsert or self.key not in data:
            data[self.key] = current_ts(self.millis)
        return data


class UUID(DictProcessorBase):
    """"基于UUID生成ID"""
    def __init__(self, key: str = '_id', upsert: bool = False):
        self.key = key
        self.upsert = upsert

    def on_data(self, data: dict, *args):
        if self.upsert or self.key not in data:
            data[self.key] = str(uuid.uuid4())
        return data


class MinValue(DictProcessorBase):
    """对指定字段获取最小值"""
    def __init__(self, target_key: str, *source_keys):
        super().__init__()
        self.target_key = target_key
        self.source_keys = source_keys

    def on_data(self, data: dict, *args):
        vals = [data[k] for k in self.source_keys if data.get(k)]
        data[self.target_key] = min(vals)
        return data


class MaxValue(DictProcessorBase):
    """对指定字段获取最大值"""
    def __init__(self, target_key: str, *source_keys):
        super().__init__()
        self.target_key = target_key
        self.source_keys = source_keys

    def on_data(self, data: dict, *args):
        vals = [data[k] for k in self.source_keys if data.get(k)]
        data[self.target_key] = max(vals)
        return data


class ReduceBase(JsonIterator):
    """对数据进行规约(many->1/0) 向后传递规约结果"""
