from typing import Any


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
