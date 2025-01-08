from wikidata_filter.iterator.base import JsonIterator, DictProcessorBase
from wikidata_filter.util.dates import current_ts
import uuid


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
    def on_data(self, data, *args):
        print(id(data), data)
        return data


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
    def __init__(self, key: str, millis: bool = True):
        self.key = key
        self.millis = millis

    def on_data(self, data: dict, *args):
        data[self.key] = current_ts(self.millis)
        return data


class PrefixID(DictProcessorBase):
    """基于已有字段生成带前缀的ID"""
    def __init__(self, prefix: str, *keys, key: str = '_id'):
        self.key = key
        self.prefix = prefix
        self.keys = keys

    def on_data(self, data: dict, *args):
        parts = [str(data.get(key)) for key in self.keys]
        data[self.key] = self.prefix + '_'.join(parts)
        return data


class UUID(DictProcessorBase):
    """"基于UUID生成ID"""
    def __init__(self, key: str = '_id'):
        self.key = key

    def on_data(self, data: dict, *args):
        data[self.key] = str(uuid.uuid4())
        return data
