"""对数据进行采样输出"""
from random import random

from wikidata_filter.util.mod_util import load_util
from wikidata_filter.util.database.base import Database
from wikidata_filter.iterator.base import JsonIterator


class Filter(JsonIterator):
    """
    过滤节点（1->?)
    根据提供的匹配函数判断是否继续往后面传递数据
    """
    def __init__(self, matcher=None, key: str = None, **kwargs):
        super().__init__()
        self.matcher = load_util(matcher) or self
        self.key = key
        self.kwargs = kwargs

    def on_data(self, data, *args):
        if self.key and self.key not in data:
            print(f"Warning: `{self.key}` not exists")
        val = data
        if self.key:
            val = data[self.key]
        if self.matcher(val):
            return data

    def __call__(self, val, *args, **kwargs):
        return self.matcher(val)


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
    def __init__(self, key: str = None):
        super().__init__(self, key)
        self.cache = set()

    def __call__(self, val, *args, **kwargs):
        r = self.exists(val)
        if r:
            print("Exist, ignore:", val)
        return not r

    def exists(self, val):
        """判断特定的值是否存在 重写此方法实现更加持久性的判断"""
        if val in self.cache:
            return True
        self.cache.add(val)
        return False


class DistinctByDatabase(Distinct):
    """基于指定的数据库表进行去重 查询结果将缓存在本地以复用"""
    def __init__(self, db_client: Database, key: str = None, **kwargs):
        super().__init__(key)
        self.db_client = db_client
        self.kwargs = kwargs

    def exists(self, val):
        if super().exists(val):
            return True
        r = self.db_client.exists(val, **self.kwargs)
        if r:
            self.cache.add(val)
        return r


class TakeN(Filter):
    """取前n条数据"""
    def __init__(self, n: int = 10):
        super().__init__(self)
        self.n = n
        self.i = -1

    def __call__(self, *args, **kwargs):
        self.i += 1
        return self.i < self.n


class SkipN(Filter):
    """跳过条数据"""
    def __init__(self, n: int = 0):
        super().__init__(self)
        self.n = n
        self.i = 0

    def __call__(self, *args, **kwargs):
        self.i += 1
        return self.i > self.n


class FieldsExist(Filter):
    """存在指定字段的过滤器"""
    def __init__(self, *keys):
        super().__init__(self)
        self.keys = keys

    def __call__(self, data: dict, *args, **kwargs):
        for key in self.keys:
            if key not in data:
                return False
        return True


class FieldsNonEmpty(Filter):
    """字段不为空的过滤器"""
    def __init__(self, *keys):
        super().__init__(self)
        self.keys = keys

    def __call__(self, data: dict, *args, **kwargs):
        for key in self.keys:
            if not data.get(key):
                return False
        return True


class All(Filter):
    """All组合过滤器 全部满足才能通过 否则不通过"""
    def __init__(self, *filters, key: str = None, **kwargs):
        super().__init__(key=key, **kwargs)
        self.filters = filters

    def __call__(self, val, *args, **kwargs):
        for one in self.filters:
            if not one(val):
                return False
        return True


class Any(Filter):
    """Any组合过滤器 任意一个满足即可通过 否则不通过"""
    def __init__(self, *filters, key: str = None, **kwargs):
        super().__init__(key=key, **kwargs)
        self.filters = filters

    def __call__(self, val, *args, **kwargs):
        for one in self.filters:
            if one(val):
                return True
        return False


class Not(Filter):
    """反转过滤器 基于已有过滤器取反"""
    def __init__(self, that: str or Filter, key: str = None, **kwargs):
        super().__init__(key=key, **kwargs)
        self.that = load_util(that)

    def __call__(self, val, *args, **kwargs):
        return not self.that(val, *args, **kwargs)
