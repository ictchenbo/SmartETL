import time
from typing import Iterable, Any
from types import GeneratorType
from random import random
from smartetl.util.dates import current_time


class Loader:
    """数据提供器接口 为流程供给数据"""
    def iter(self) -> Iterable[Any]:
        pass

    def __call__(self, *args, **kwargs):
        return self.iter()

    def close(self):
        pass

    @property
    def name(self):
        return self.__class__.__name__

    def __str__(self):
        return f'{self.name}'


class Array(Loader):
    """基于数组提供数据"""
    def __init__(self, data: list):
        self.data = data

    def iter(self):
        for item in self.data:
            yield item


class String(Loader):
    """基于文本提供数据 按照指定分隔符进行分割"""
    def __init__(self, text: str, sep: str = '\n'):
        self.data = text
        self.sep = sep

    def iter(self):
        for item in self.data.split(self.sep):
            yield item


class Input(Loader):
    """通过用户输入提供数据"""
    def __init__(self, msg: str = None):
        self.msg = msg or "请输入（`exit`退出）: "

    def iter(self):
        while True:
            line = input(self.msg).strip()
            if line == "exit":
                break
            if line:
                yield line


class Random(Loader):
    """随机生成器"""
    def __init__(self, num_of_times: int = 0):
        self.num_of_times = num_of_times

    def iter(self):
        if self.num_of_times > 0:
            for i in range(self.num_of_times):
                yield random()
        else:
            while True:
                yield random()


class TimedLoader(Loader):
    """定时轮询器 定时无限（或指定次数）调用提供的Loader 比如定时进行数据库轮询或接口轮询"""
    def __init__(self, that: Loader, interval: int = 15, num_of_times: int = 0):
        self.that = that
        self.interval = interval
        self.num_of_times = num_of_times

    def iter(self):
        counter = 0
        while True:
            print(f"{self} running at: ", current_time())
            counter += 1
            for item in self.that.iter():
                yield item

            if 0 < self.num_of_times <= counter:
                break

            time.sleep(self.interval)

    def __str__(self):
        return f"TimedPull[{self.that.name}, interval={self.interval}]"


class Function(Loader):
    """函数调用包装器 提供调用函数的结果"""
    def __init__(self, function, *args, **kwargs):
        """
        :param function 函数对象或函数对象的完整限定名（如wikidata_filter.util.files.get_lines）
        """
        assert function is not None, "function is None!"
        if isinstance(function, str):
            from smartetl.util.mod_util import load_cls
            function = load_cls(function)[0]
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def iter(self):
        res = self.function(*self.args, **self.kwargs)
        if isinstance(res, GeneratorType):
            for item in res:
                yield item
        else:
            yield res


class QueueLoader(Loader):
    """基于本地队列的加载器"""
    def __init__(self, timeout: int = 60):
        self.queue = []
        self.timeout = timeout

    def iter(self):
        while self.queue:
            item = self.queue.pop(0)
            yield item


class MultiLoader(Loader):
    """组合多个loader"""
    def __init__(self, *loaders: Loader):
        self.loaders = loaders

    def iter(self):
        for loader in self.loaders:
            res = loader.iter()
            if isinstance(res, GeneratorType):
                for item in res:
                    yield item
            else:
                yield res
