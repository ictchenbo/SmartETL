from typing import Iterable, Any
import json
import yaml
from wikidata_filter.util.files import open_file
from wikidata_filter.loader.file import File


class TextBase(File):
    """文本文件基类，可加载整个文件作为一个字符串输出 仅适合小文件"""
    def __init__(self, input_file: str, encoding: str = "utf8", **kwargs):
        self.input_file = input_file
        self.instream = open_file(input_file, mode="r", encoding=encoding, **kwargs)

    def iter(self) -> Iterable[Any]:
        yield self.instream.read()


class Yaml(TextBase):
    """加载yaml文件作为一个dict对象 仅适合小文件"""
    def __init__(self, input_file: str,  **kwargs):
        super().__init__(input_file, **kwargs)

    def iter(self):
        yield yaml.load(self.instream, Loader=yaml.FullLoader)


class Json(TextBase):
    """整个文件作为一个JSON对象（不管是dict还是list），仅适合小文件"""
    def __init__(self, input_file: str, **kwargs):
        super().__init__(input_file, **kwargs)

    def iter(self):
        content = self.instream.read()
        yield json.loads(content)


class JsonArray(TextBase):
    """
    整个文件作为JsonArray，输出数组中的每一项，仅适合小文件
    """
    def __init__(self, input_file: str, **kwargs):
        super().__init__(input_file, **kwargs)

    def iter(self):
        content = self.instream.read()
        json_array = json.loads(content)
        for item in json_array:
            yield item


# ---------------------以下为按行输出的文本文件---------------------


class Text(TextBase):
    """输出文本文件的每一行"""
    def __init__(self, input_file: str, **kwargs):
        super().__init__(input_file, **kwargs)

    def iter(self):
        for line in self.instream:
            yield line


class JsonLine(Text):
    """
     Json行文件加载器
    """
    def __init__(self, input_file: str, **kwargs):
        super().__init__(input_file, **kwargs)

    def iter(self):
        for line in super().iter():
            yield json.loads(line)


class JsonFree(Text):
    """对格式化JSON文件进行加载 自动检测边界。【注意】此加载器可能不够鲁棒"""
    def __init__(self, input_file: str, **kwargs):
        super().__init__(input_file, **kwargs)

    def iter(self):
        lines = []
        for line in super().iter():
            line_s = line.rstrip()
            if lines:
                lines.append(line_s)
                # 遇到]或}行 认为是JSON对象或JSON数组的结束
                if line_s == ']' or line_s == '}':
                    one = json.loads(''.join(lines))
                    yield one
                    lines.clear()
            else:
                if not line_s:
                    continue
                lines.append(line_s)


class CSV(Text):
    """读取CSV文件 每行作为一个对象传输"""
    def __init__(self, input_file: str, sep: str = ',', header: bool = True, **kwargs):
        super().__init__(input_file, **kwargs)
        self.header = header
        self.sep = sep

    def iter(self):
        try:
            import csv
        except ImportError:
            raise Exception("failed to import csv")
        reader = csv.reader(super().iter())
        header = None
        for index, row in enumerate(reader):
            if self.header:
                if index == 0:
                    header = row
                else:
                    yield dict(zip(header, row))
            else:
                yield row
