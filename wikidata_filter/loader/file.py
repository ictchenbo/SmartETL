import os
from wikidata_filter.loader.base import DataProvider


class File(DataProvider):
    """
    文件加载器
    """
    instream = None
    input_file: str = None

    def close(self):
        if self.instream:
            self.instream.close()
            self.instream = None

    def __str__(self):
        return f"{self.name}('{self.input_file}')"


class BinaryFile(File):
    """二进制文件基类 根据需要自动按照rb模式打开文件"""
    def __init__(self, input_file: str, auto_open: bool = True, **kwargs):
        assert os.path.exists(input_file) and os.path.isfile(input_file), f"文件不存在或不是文件: {input_file}"
        if auto_open:
            self.instream = open(input_file, "rb")
        self.input_file = input_file
