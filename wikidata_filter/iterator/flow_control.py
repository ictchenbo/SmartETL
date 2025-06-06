import traceback
from types import GeneratorType
from typing import Any

from wikidata_filter.iterator.base import JsonIterator, Message
from wikidata_filter.util.dicts import copy_val
from wikidata_filter.util.mod_util import load_cls


class Multiple(JsonIterator):
    """多个节点组合"""
    def __init__(self, *nodes):
        """
        :param *nodes 处理算子
        """
        self.nodes = list(nodes)

    def add(self, iterator: JsonIterator):
        """添加节点"""
        self.nodes.append(iterator)
        return self

    def on_start(self):
        for it in self.nodes:
            it.on_start()

    def on_complete(self):
        for it in self.nodes[::-1]:
            it.on_complete()

    def __str__(self):
        nodes = [it.__str__() for it in self.nodes]
        return f'{self.name}(nodes={nodes})'
        # return f'{self.name}()'


class Fork(Multiple):
    """
    分叉节点（并行逻辑），各处理节点独立运行。
    Fork节点本身不产生输出，因此不能与其他节点串联
    """
    def __init__(self, *nodes, copy_data: bool = False):
        """
        :param *nodes 处理算子
        :param copy_data 是否复制数据，使得各个分支对数据修改互不干扰
        """
        super().__init__(*nodes)
        self.copy_data = copy_data

    def __process__(self, data: Any, *args):
        for node in self.nodes:
            _data = data
            if self.copy_data:
                _data = copy_val(_data)
            res = node.__process__(_data, *args)
            # 注意：包含yield的函数调用仅返回迭代器，而不会执行函数
            if isinstance(res, GeneratorType):
                for _ in res:
                    pass


class Chain(Multiple):
    """
    链式组合节点（串行逻辑），前一个的输出作为后一个的输入。
    """
    def __init__(self, *nodes):
        super().__init__(*nodes)

    def walk(self, data: Any, break_when_empty: bool = True, end_msg: bool = False) -> list[Any]:
        """将前一个节点的输出作为下一个节点的输入，依次执行每个节点。返回最后一个节点的输出"""
        queue = [data]
        for node in self.nodes:
            new_queue = []  # cache for next processor, though there's only one item for most time
            # iterate over the current cache
            for current in queue:
                try:
                    res = node.__process__(current)
                except Exception as e:
                    print("ERROR! node: ", node, "data:", current)
                    traceback.print_exc()
                    raise e
                # 注意：包含yield的函数调用仅返回迭代器，而不会执行函数
                if isinstance(res, GeneratorType):
                    for one in res:
                        if one is not None:
                            new_queue.append(one)
                else:
                    if res is not None:
                        new_queue.append(res)

            # empty, check if break the chain
            if not new_queue and break_when_empty:
                return new_queue

            if end_msg:
                # send END msg in the end
                new_queue.append(None)

            queue = new_queue
        return queue

    def __process__(self, data: Any, *args):
        # print('Chain.__process__', data)
        # 普通流程中如果收到None 则中断执行链条
        if data is None:
            return None

        # 特殊消息处理
        if isinstance(data, Message):
            # 收到结束消息 后续节点还需要
            if data.msg_type == 'end':
                # print(f'{self.name}: END/Flush signal received.')
                queue = self.walk(data.data, break_when_empty=False, end_msg=True)
                if queue:
                    for one in queue:
                        yield one
                return
            else:
                data = data.data

        # 返回结果 从而支持与其他节点串联
        queue = self.walk(data)
        if queue:
            for one in queue:
                yield one


class If(JsonIterator):
    """流程选择节点，指定条件满足时执行"""

    def __init__(self, node: JsonIterator, matcher=None, key: str = None):
        assert node, "node is None"
        assert matcher or key, "matcher and key both None"
        if matcher is None:
            matcher = lambda r: r.get(key)
        elif isinstance(matcher, str):
            matcher = load_cls(matcher)[0]
        self.matcher = matcher
        self.node = node

    def __process__(self, data: Any, *args):
        if data is None:
            return None
        if isinstance(data, Message):
            if data.msg_type == 'end':
                return self.node.__process__(data)
            data = data.data

        if not self.matcher(data):
            return data

        return self.node.__process__(data)


class IfElse(JsonIterator):
    """流程选择节点，指定条件满足时执行node_a，否则执行node_b"""

    def __init__(self, node_a: JsonIterator, node_b: JsonIterator, matcher=None, key: str = None):
        assert node_a and node_b, "node_a or node_b is None"
        assert matcher or key, "matcher and key both None"
        if matcher is None:
            matcher = lambda r: r.get(key)
        elif isinstance(matcher, str):
            matcher = load_cls(matcher)[0]
        self.matcher = matcher
        self.node_a = node_a
        self.node_b = node_b

    def __process__(self, data: Any, *args):
        if data is None:
            return None
        if isinstance(data, Message):
            if data.msg_type == 'end':
                self.node_a.__process__(data)
                self.node_b.__process__(data)
                return None
            data = data.data
        if self.matcher(data):
            res = self.node_a.__process__(data)
        else:
            res = self.node_b.__process__(data)

        return res


class While(If):
    """循环节点，重复执行某个节点，直到条件不满足"""

    def __init__(self, node: JsonIterator, matcher=None, key: str = None, max_iterations: int = -1):
        super().__init__(node, matcher=matcher, key=key)
        self.max_iterations = max_iterations

    def __process__(self, data: Any, *args):
        if data is None:
            return None
        if isinstance(data, Message):
            if data.msg_type == 'end':
                res = self.node.__process__(data)
                if isinstance(res, GeneratorType):
                    for one in res:
                        if one is not None:
                            yield one
                else:
                    yield res
                return
            data = data.data

        ith = 0
        queue = [data]
        while queue:
            new_queue = []
            unfinished = False
            for one in queue:
                if one and self.matcher(one):
                    unfinished = True
                    res = self.node.__process__(one)
                    if isinstance(res, GeneratorType):
                        for one2 in res:
                            if one2 is not None:
                                new_queue.append(one2)
                    else:
                        new_queue.append(res)
            if not unfinished:
                break
            else:
                queue = new_queue
                ith += 1
                if 0 < self.max_iterations <= ith:
                    break

        for one in queue:
            yield one
