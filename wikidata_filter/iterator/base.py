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


class Wait(JsonIterator):
    """延时处理"""
    def __init__(self, seconds: int = 1):
        self.seconds = seconds

    def on_data(self, data: Any, *args):
        import time
        time.sleep(self.seconds)
        return data


class WriteQueue(JsonIterator):
    """写入队列"""
    def __init__(self, queue):
        self.queue = queue

    def on_data(self, data: Any, *args):
        self.queue.append(data)
        return data
