"""对数据进行采样输出"""
from random import random
from wikidata_filter.util.jsons import extract as V
from wikidata_filter.iterator.base import JsonIterator


class Filter(JsonIterator):
    """
    过滤节点（1->?)
    根据提供的匹配函数判断是否继续往后面传递数据
    """
    def __init__(self, matcher, key: str = None):
        super().__init__()
        assert matcher is not None, "matcher should not be None"
        self.matcher = matcher
        self.key = key

    def on_data(self, data, *args):
        if self.key and self.key not in data:
            print(f"Warning: `{self.key}` not exists")
        val = data
        if self.key:
            val = data[self.key]
        if self.matcher(val):
            return data


class WhiteList(Filter):
    """白名单过滤 匹配白名单的才保留"""
    def __init__(self, cache: dict or set, key: str):
        super().__init__(self, key)
        self.cache = cache

    def __call__(self, val, *args, **kwargs):
        return val in self.cache


class BlackList(Filter):
    """黑名单过滤 匹配黑名单的被过滤掉"""
    def __init__(self, cache: dict or set, key: str):
        super().__init__(self, key)
        self.cache = cache

    def __call__(self, val, *args, **kwargs):
        return val not in self.cache


class Sample(Filter):
    """随机采样 保留特定比例的数据"""
    def __init__(self, rate: float = 0.1):
        super().__init__(self)
        self.rate = rate

    def __call__(self, *args, **kwargs):
        return random() <= self.rate


class Distinct(Filter):
    """去重，过滤掉重复数据 默认为本地set进行缓存判重，可实现基于内存数据库（如redis）或根据业务数据库进行重复监测"""
    def __init__(self, key: str):
        super().__init__(self)
        assert key is not None, "key 不能为空"
        self.field = key
        self.cache = set()

    def __call__(self, data: dict):
        val = V(data, self.field)
        return not self.exists(val)

    def exists(self, val):
        """判断特定的值是否存在 重写此方法实现更加持久性的判断"""
        if val in self.cache:
            return True
        self.cache.add(val)
        return False


class DistinctRedis(Distinct):
    """基于Redis进行缓存去重 具体待实现"""
    def __init__(self, key: str):
        super().__init__(key)
        raise NotImplemented()

    def exists(self, val):
        pass


class TakeN(Filter):
    """取前n条数据"""
    def __init__(self, n: int = 10):
        super().__init__(self)
        self.n = n
        self.i = -1

    def __call__(self, *args, **kwargs):
        self.i += 1
        return self.i < self.n
