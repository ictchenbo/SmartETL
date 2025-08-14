"""对数据进行采样输出"""
from random import random

from smartetl.util.mod_util import load_util
from smartetl.util.database.base import Database
from smartetl.util.dates import current_ts
from smartetl.iterator.base import JsonIterator


class Filter(JsonIterator):
    """
    过滤节点（1->?)
    根据提供的匹配函数判断是否继续往后面传递数据 函数返回True表示保留数据 False表示丢弃数据
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
        return self.matcher(val, *args, **kwargs)


class WhiteList(Filter):
    """白名单过滤 匹配白名单的才保留"""
    def __init__(self, cache: dict or set, key: str):
        super().__init__(self, key)
        self.cache = cache
        print('WhiteList init: total', len(cache), 'items')

    def __call__(self, val, *args, **kwargs):
        # print('current', val)
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
    def __init__(self, db_client: Database, key: str = None, local_cache: bool = True, **kwargs):
        super().__init__(key)
        self.db_client = db_client
        self.local_cache = local_cache
        self.kwargs = kwargs

    def exists(self, val):
        if not self.local_cache:
            return self.db_client.exists(val, **self.kwargs)
        if val in self.cache:
            return True
        r = self.db_client.exists(val, **self.kwargs)
        if r:
            # 缓存到本地 加速后续查找
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


class FilterByTime(Filter):
    """根据当前时间戳值进行过滤"""
    def __init__(self, key: str = None, offset_days: int = 0, **kwargs):
        super().__init__(key=key, **kwargs)
        self.offset_days = offset_days
        self.offset_millis = offset_days * 86400000

    def __call__(self, val, *args, **kwargs):
        return isinstance(val, int) and val <= current_ts(millis=True) + self.offset_millis


class KeywordFilter(Filter):
    """根据关键词进行匹配过滤"""
    def __init__(self, key: str = None, keywords: list = None, action: str = "drop", **kwargs):
        super().__init__(key=key, **kwargs)
        self.keywords = [kw.strip().lower() for kw in keywords]
        self.keep = action == "keep"

    def __call__(self, val: str, *args, **kwargs):
        for kw in self.keywords:
            if kw in val.lower():
                return self.keep
        return not self.keep


class KeywordFilterV2(Filter):
    """根据自动机的关键词匹配过滤"""
    def __init__(self, key: str = None, keywords: list = None, action: str = "drop", **kwargs):
        super().__init__(key=key, **kwargs)
        self.keep = action == "keep"
        from ahocorasick import Automaton
        self.actree = Automaton()
        for word in keywords:
            word = word.strip()
            self.actree.add_word(word, word)
        self.actree.make_automaton()

    def __call__(self, val, *args, **kwargs):
        for _ in self.actree.iter_long(val):
            return self.keep
        return not self.keep


class LengthFilter(Filter):
    def __init__(self, key: str = None, min_length: int = 1, max_length: int = None, **kwargs):
        super().__init__(key=key, **kwargs)
        self.min_length = min_length
        self.max_length = max_length

    def __call__(self, val, *args, **kwargs):
        if 0 < self.min_length and self.min_length > len(val):
            return False
        if self.max_length is not None and len(val) > self.max_length:
            return False
        return True
