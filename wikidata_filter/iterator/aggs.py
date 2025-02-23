"""本模块为统计分析 提供基于聚合数据的基本统计算子"""
from typing import Any
from random import random
from wikidata_filter.util.jsons import extract, get_valid as V
from .aggregation import ReduceBase


class Reduce(ReduceBase):
    """规约，根据提供的函数进行数据规约。注意，此类算子在Group(或其子类）处理之后"""
    def __init__(self, func, source_key: str = 'values', target_key: str = None):
        """
        :param func (rows) -> res 规约函数
        :param source_key 指定输入字段名 默认为values（Group默认的结果数据）
        :param target_key 指定结果字段 默认为None 表示与source_key相同 如果需要保留原分组 则可以需要提供一个不同的字段名
        """
        self.func = func
        self.source_key = source_key
        self.target_key = target_key or source_key

    def on_data(self, data: Any, *args):
        if not isinstance(data, dict):
            print(f"{self.name} Warning: the data must be dict")
        elif self.source_key not in data:
            print(f"{self.name} Warning: {self.source_key} not exists, all keys:", data.keys())
        else:
            data[self.target_key] = self.func(data[self.source_key])
        return data


class ReduceBy(Reduce):
    """提供一个初始函数和加法函数进行规约"""
    def __init__(self, init_func, add_func):
        super().__init__(self)
        self.init_func = init_func
        self.add_func = add_func

    def __call__(self, items: list):
        result = self.init_func(items)
        for item in items:
            result = self.add_func(result, item)

        return result


class Count(Reduce):
    """group计数 ，等价SQL：select count() from group"""
    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)

    def __call__(self, items: list):
        return len(items)


class Sum(Reduce):
    """group求和"""
    def __init__(self, field: str, init: float or int = 0):
        super().__init__(self)
        assert field is not None, "field 不能为空"
        self.field = field
        self.init = init

    def __call__(self, items: list):
        return self.init + sum(V(items, self.field))


class Min(Reduce):
    """group求最小"""
    def __init__(self, field: str):
        super().__init__(self)
        assert field is not None, "field 不能为空"
        self.field = field

    def __call__(self, items: list):
        return min(V(items, self.field))


class Max(Reduce):
    """group求最大"""
    def __init__(self, field: str):
        super().__init__(self)
        assert field is not None, "field 不能为空"
        self.field = field

    def __call__(self, items: list):
        return max(V(items, self.field))


class Mean(Reduce):
    """group求平均"""
    def __init__(self, field: str):
        super().__init__(self)
        assert field is not None, "field 不能为空"
        self.field = field

    def __call__(self, items: list):
        valid_items = V(items, self.field)
        if valid_items:
            return sum(valid_items) / len(valid_items)
        return None


class Var(Reduce):
    """group计算方差"""
    def __init__(self, field: str):
        super().__init__(self)
        assert field is not None, "field 不能为空"
        self.field = field

    def __call__(self, items: list):
        import numpy
        return numpy.std(V(items, self.field))


class Std(Reduce):
    """group计算标准差"""
    def __init__(self, field: str):
        super().__init__(self)
        assert field is not None, "field 不能为空"
        self.field = field

    def __call__(self, items: list):
        import numpy
        return numpy.std(V(items, self.field))


class Head(Reduce):
    """group分组取前若干项，等价SQL：select * from group limit num"""
    def __init__(self, num: int = 10):
        super().__init__(self)
        self.num = num

    def __call__(self, items: list):
        return items[:self.num]


class Tail(Reduce):
    """group取分组最后取若干项，等价SQL：select * from group limit N-num, num"""
    def __init__(self, num: int = 10):
        super().__init__(self)
        self.num = num

    def __call__(self, items: list):
        return items[:-self.num:]


class Sample(Reduce):
    """group分组内数据进行采样"""
    def __init__(self, rate: float = 0.01):
        super().__init__(self)
        self.rate = rate

    def __call__(self, items: list):
        return [item for item in items if random() <= self.rate]


class OrderBy(Reduce):
    """group分组排序，等价SQL：select * from group order by field asc/desc"""
    def __init__(self, field: str, descend: bool = False):
        super().__init__(self)
        assert field is not None, "field 不能为空"
        self.field = field
        self.descend = descend

    def __call__(self, items: list):
        return sorted(items, key=lambda i: V(i, self.field), reverse=self.descend)


class Distinct(Reduce):
    """group分组根据指定字段进行去重，等价SQL：select distinct(field) from group"""
    def __init__(self, field: str):
        super().__init__(self)
        assert field is not None, "field 不能为空"
        self.field = field

    def __call__(self, items: list):
        s = set()
        for item in items:
            s.add(extract(item, self.field))
        return list(s)
