from typing import Any

from wikidata_filter.iterator.aggregation import ReduceBase


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
