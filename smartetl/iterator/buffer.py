"""本模块为聚合分析相关算子 提供分组、缓存"""
from smartetl.iterator.base import Message, ReduceBase
from smartetl.util.database.base import Database


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
        if isinstance(item, Message):
            item = None if item.msg_type == "end" else item.data

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
    def __init__(self, buffer_size=1000, mode="single", **kwargs):
        super().__init__(buffer_size, mode=mode)

    def commit(self):
        self.write_batch(self.buffer)

    def write_batch(self, data: list):
        pass


class DatabaseWriter(BufferedWriter):
    """基于数据库的缓存写；等价于  Chain(Buffer, gestata.dbops.upsert)"""
    def __init__(self, db: Database, write_mode: str = "upsert", **kwargs):
        super().__init__(**kwargs)
        self.db = db
        self.write_mode = write_mode
        self.kwargs = dict(**kwargs)
        if "buffer_size" in self.kwargs:
            kwargs.pop("buffer_size")
        if "mode" in self.kwargs:
            kwargs.pop("mode")

    def write_batch(self, rows: list):
        self.db.upsert(rows, write_mode=self.write_mode, **self.kwargs)
        # print(f"Write {n} rows")
