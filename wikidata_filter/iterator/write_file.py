"""输出到文件的算子"""
import json
from wikidata_filter.iterator.aggregation import BufferedWriter


class WriteText(BufferedWriter):
    """带缓冲的文本文件写，通常换行分隔。由于文件IO自带缓冲，通常不需要这么做，但可以支持更好的写入性能"""
    writer = None

    def __init__(self, output_file: str, append: bool = False, encoding: str = "utf8", buffer_size: int = 1000, sep: str = '\n', mode=None):
        """
        :param output_file 输出文件
        :param append 是否追加模式(a) 默认为否(w)
        :param encoding 编码 文本内容的编码方式 默认为utf8（推荐）
        :param buffer_size 缓冲大小 默认1000
        :param sep 行分隔符 默认为 '\n'
        :param mode 压缩模式 默认为None（不启用压缩） 支持gzip
        """
        super().__init__(buffer_size=buffer_size)
        self.output_file = output_file
        self.sep = sep
        self.append = append
        self.encoding = encoding
        self.mode = mode
        if mode == "gzip" and not output_file.endswith(".gz"):
            self.output_file = output_file + ".gz"
            print("in gzip mode, set file prefix .gz")

    def write_batch(self, data: list):
        # lazy ini
        if self.writer is None:
            if not self.mode:
                self.writer = open(self.output_file, 'a' if self.append else 'w', encoding=self.encoding)
            elif self.mode == "gzip" and not self.append:
                import gzip
                self.writer = gzip.open(self.output_file, "wt", encoding=self.encoding)
        lines = [self.serialize(item) for item in data]
        content = self.sep.join(lines)
        if self.mode == "gzip" and self.append:
            import gzip
            with gzip.open(self.output_file, 'ab') as writer:
                writer.write(content.encode(self.encoding))
                writer.write(self.sep.encode(self.encoding))
        else:
            self.writer.write(content)
            self.writer.write(self.sep)
            self.writer.flush()
        print('batch written to file')

    def serialize(self, item) -> str:
        """序列化为文本"""
        return str(item)

    def on_complete(self):
        super().on_complete()
        if self.writer:
            self.writer.close()

    def __str__(self):
        return f"{self.name}(output_file='{self.output_file}', sep='{self.sep}', append={self.append}, encoding='{self.encoding}', buffer_size={self.buffer_size})"


class WriteJson(WriteText):
    """
    写JSON文件
    """
    def __init__(self, output_file: str, **kwargs):
        super().__init__(output_file, **kwargs)

    def serialize(self, item) -> str:
        return json.dumps(item, ensure_ascii=False)


class WriteCSV(WriteText):
    """
    写CSV文件
    """
    def __init__(self, output_file: str, keys: list = None, seperator=','):
        super().__init__(output_file)
        self.keys = keys
        self.seperator = seperator

    def serialize(self, item) -> str:
        line = []
        if self.keys is None:
            for k, v in item.items():
                line.append(str(v))
        else:
            for k in self.keys:
                self.writer.write(str(item.get(k)))

        return self.seperator.join(line)
