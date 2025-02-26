"""本模块为聚合分析相关算子 提供分组、缓存"""
from wikidata_filter.iterator.base import JsonIterator


class ReduceBase(JsonIterator):
    """对数据进行规约(many->1/0) 向后传递规约结果"""


class Buffer(ReduceBase):
    """
    缓冲节点 当到达的数据填满缓冲池后再一起向后传递。主要用于批处理场景。
    未来可以扩展 如带时限的缓冲
    """
    def __init__(self, buffer_size=100, mode="batch"):
        """
        :param buffer_size 缓冲池大小
        :param mode 数据传递模式，batch表示批量传递 否则为普通模式 默认为batch模式
        """
        self.buffer_size = buffer_size
        self.buffer = []
        self.mode = mode

    def __process__(self, item: dict or None, *args):
        if item is None:
            # 数据结束或需要刷新缓存
            # print(f'{self.name}: END/Flush signal received.')
            if self.buffer:
                self.commit()
            if self.mode == "batch":
                # 批量模式下 需要发送最后一批数据（发送前缓存置空）
                tmp = self.buffer
                self.buffer = []
                return tmp
            else:
                # 单条模式下 由于之前每条数据都发送了 这里无需发送
                self.buffer.clear()
                return None
        self.buffer.append(item)
        # print(f'add to buffer {len(self.buffer)}/{self.buffer_size}')
        # if full
        if len(self.buffer) >= self.buffer_size:
            # print('buffer full, commit')
            self.commit()
            if self.mode == "batch":
                tmp = self.buffer
                self.buffer = []
                return tmp
            else:
                self.buffer.clear()
                return item
        else:
            if self.mode == "batch":
                return None
            else:
                return item

    def commit(self):
        """提交当前缓冲数据 比如写入数据库或文件"""
        print("buffer data commited, num of rows:", len(self.buffer))

    def on_complete(self):
        if self.buffer:
            self.commit()
            self.buffer.clear()

    def __str__(self):
        return f"{self.name}(buffer_size={self.buffer_size}, mode='{self.mode}')"


class BufferedWriter(Buffer):
    """缓冲写 比如数据库或文件算子 默认作为普通算子加入流程"""
    def __init__(self, buffer_size=1000, mode="single"):
        super().__init__(buffer_size, mode=mode)

    def commit(self):
        self.write_batch(self.buffer)

    def write_batch(self, data: list):
        pass


class Group(ReduceBase):
    """分组规约，基于指定字段的值进行分组"""
    def __init__(self, by: str, emit_fast: bool = True):
        """
        :param by 指定字段的key
        :param emit_fast 是否快速提交数据 如果遇到了不同的键值。默认为True
        """
        super().__init__()
        self.by = by
        self.emit_fast = emit_fast
        self.groups = {}
        self.last_key = None

    def __process__(self, data: dict or None, *args):
        # print('Group.__process__', data)
        if data is None:
            # print(f'{self.name}: END/Flush signal received.')
            for key, values in self.groups.items():
                print('grouping key:', key)
                yield dict(key=key, values=values)
            self.groups.clear()
            self.last_key = None
        else:
            group_key = data.get(self.by)
            # print('group_key:', group_key)
            if group_key is None:
                yield None
            group_key = str(group_key)
            if group_key in self.groups:
                self.groups[group_key].append(data)
            else:
                last_k = self.last_key
                if self.emit_fast and last_k is not None:
                    print('grouping key:', last_k)
                    last_v = self.groups.pop(last_k)
                    yield dict(key=last_k, values=last_v)

                self.last_key = group_key
                self.groups[self.last_key] = [data]

            yield None

    def __str__(self):
        return f"{self.name}(by='{self.by}', emit_fast={self.emit_fast})"
