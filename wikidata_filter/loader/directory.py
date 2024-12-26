import os
from typing import Iterable, Any

from wikidata_filter.loader.base import DataProvider
from .text import Text, CSV, Json, JsonLine, JsonArray, JsonFree, Yaml
from .html import HTML
# from .xls import ExcelStream
# from .parquet import Parquet
# from .docx import Doc, Docx
# from .pptx import PPT, PPTX
# from .pdf import PDF


LOADERS = {
    '.txt': Text,
    '.csv': CSV,
    '.yaml': Yaml,
    '.yml': Yaml,
    '.json': Json,
    '.jsonl': JsonLine,
    '.jsona': JsonArray,
    '.jsonf': JsonFree,
    '.html': HTML,
    # '.xls': ExcelStream,
    # '.parquet': Parquet,
    # '.doc': Doc,
    # '.docx': Docx,
    # '.pdf': PDF,
    # '.ppt': PPT,
    # '.pptx': PPTX
}


def mk_builder(_type: str, *args, **kwargs):
    assert _type in LOADERS, f"{_type} file not supported"
    return LOADERS[_type]


class Directory(DataProvider):
    """扫描文件夹 对指定后缀的文件按照默认参数进行读取 返回 (filename, data_row)"""
    def __init__(self, path: str or list, *suffix, recursive: bool = False, type_mapping: dict = None, **kwargs):
        """
        :param path 指定文件夹路径（单个或数组）
        :param *suffix 文件的后缀 'all' 表示全部
        :param recursive 是否递归遍历文件夹
        :param type_mapping 类型映射 例如{'.json':'.jsonl' }表示将.json文件当做.jsonl（JSON行）文件处理
        """
        self.path = path
        if isinstance(path, str):
            self.path = [path]
        self.suffix = suffix
        self.all_file = 'all' in suffix
        self.recursive = recursive
        self.type_mapping = type_mapping or {}
        self.extra_args = kwargs

    def match_file(self, filename: str):
        # print(filename)
        if self.all_file:
            return True
        for si in self.suffix:
            if filename.endswith(si):
                return True
        return False

    def get_filetype(self, file_path: str):
        filename = os.path.split(file_path)[1]
        if '.' not in filename:
            return ''
        suffix = filename[filename.rfind('.'):]
        return self.type_mapping.get(suffix) or suffix

    def get_loader(self, file_path: str):
        filetype = self.get_filetype(file_path)
        cls = mk_builder(filetype)
        return cls(file_path, **self.extra_args)()

    def gen_doc(self, file_path):
        filename = os.path.split(file_path)[1]
        print("processing", file_path)
        for row in self.get_loader(file_path):
            yield {
                "filename": filename,
                "data": row
            }

    def iter(self) -> Iterable[Any]:
        for file_path in self.path:
            if os.path.isfile(file_path) and self.match_file(file_path):
                for row in self.gen_doc(file_path):
                    yield row
            elif os.path.isdir(file_path):
                if self.recursive:
                    for root, dirs, files in os.walk(file_path):
                        for file in files:
                            if self.match_file(file):
                                for row in self.gen_doc(os.path.join(root, file)):
                                    yield row
                else:
                    for item in os.listdir(file_path):
                        item_path = os.path.join(file_path, item)
                        if os.path.isfile(item_path) and self.match_file(item_path):
                            for row in self.gen_doc(item_path):
                                yield row

    def __str__(self):
        return f'{self.name}(path={self.path}, *{self.suffix})'

