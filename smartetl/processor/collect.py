from typing import Any

from smartetl.processor.buffer import ReduceBase


class Collect(ReduceBase):
    """
    收集节点，汇聚所有数据供后续处理
    """
    def __init__(self, mode="stop"):
        """
        :param mode 数据传递模式，stop（默认）表示不传递；bypass 通过模式；batch 批量模式；single 单条模式
        """
        self.buffer = []
        self.mode = mode

    def __process__(self, data: Any, *args):
        if data is None:
            self.data_finished()
            if self.mode == "batch":
                yield self.buffer
            elif self.mode == "single":
                for item in self.buffer:
                    yield item
            else:
                yield None
        else:
            self.buffer.append(data)

            if self.mode == "bypass":
                yield data
            else:
                yield None

    def data_finished(self):
        pass

    def get(self):
        return self.buffer

    def __str__(self):
        return f"{self.name}(mode='{self.mode}')"


class Sort(Collect):
    """对收集后的数据进行排序后"""
    def __init__(self, keyfunc=None, key: str = None, reverse: bool = False, mode="single"):
        """
        :param keyfunc 排序键函数（优先）
        :param key 指定用于排序的字段名（假设待排序数据为dict）
        :param reverse 是否倒序（默认False）
        :param mode 输出模式 stop表示不传递；bypass 通过模式；batch 批量模式；single 单条模式（默认）
        """
        super().__init__(mode=mode)
        self.key = keyfunc
        if not keyfunc and key:
            self.key = lambda r: r.get(key)
        self.reverse = reverse

    def data_finished(self):
        self.buffer.sort(key=self.key, reverse=self.reverse)
