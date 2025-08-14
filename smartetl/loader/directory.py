import os
import traceback
from typing import Iterable, Any

from smartetl.base import ROOT, LOADER_MODULE
from smartetl.util.mod_util import load_cls

from .base import DataProvider
from .file import BinaryFile
from .text import Text, CSV, Json, JsonLine, JsonArray, JsonFree, Yaml, TextPlain
from .file_entry import ALL_LOADERS


LOADERS = {
    '.raw': BinaryFile,
    '.txt': Text,
    '.csv': CSV,
    '.yaml': Yaml,
    '.yml': Yaml,
    '.json': Json,
    '.jsonl': JsonLine,
    '.jsona': JsonArray,
    '.jsonf': JsonFree,
    '.html': TextPlain,
    '.plain': TextPlain,
    '.md': TextPlain
}


class Directory(DataProvider):
    """扫描文件夹 对指定后缀的文件按照默认参数进行读取 返回 (filename, data_row)"""
    def __init__(self, path: str or list,
                 *suffix,
                 recursive: bool = False,
                 type_mapping: dict = None,
                 filename_only: bool = False,
                 data_format: str = None,
                 **kwargs):
        """
        :param path 指定文件夹路径（单个或数组）
        :param *suffix 文件的后缀 'all' 表示全部
        :param recursive 是否递归遍历文件夹
        :param filename_only 只获取文件名（及路径）
        :param type_mapping 类型映射 例如{'.json':'.jsonl' }表示将.json文件当做.jsonl（JSON行）文件处理；支持两个特殊键：'all' 所有类型都映射到这个；'other' 除了声明的其他都映射到这个
        """
        self.path = path
        if isinstance(path, str):
            self.path = [path]
        self.suffix = suffix
        self.all_file = 'all' in suffix
        self.recursive = recursive
        self.filename_only = filename_only
        if filename_only and type_mapping is not None:
            print("Warning, when filename_only=True, type_mapping would not take effect")
        self.type_mapping = type_mapping or {}
        self.data_format = data_format
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
        if 'all' in self.type_mapping:
            return self.type_mapping['all']
        filename = os.path.split(file_path)[1]
        if '.' not in filename:
            return ''
        suffix = filename[filename.rfind('.'):]
        if suffix in self.type_mapping:
            return self.type_mapping[suffix]
        if 'other' in self.type_mapping:
            return self.type_mapping['other']
        return suffix

    def mk_builder(self, _type: str, *args, **kwargs):
        assert _type in LOADERS or _type in ALL_LOADERS, f"{_type} file not supported"
        if _type in LOADERS:
            return LOADERS[_type]
        loader = load_cls(f'{ROOT}.{LOADER_MODULE}.{ALL_LOADERS[_type]}')[0]
        LOADERS[_type] = loader
        return loader

    def gen_doc(self, file_path):
        print("processing", file_path)
        filename = os.path.split(file_path)[1]
        if self.filename_only:
            # 不需要加载文件
            yield {
                "filename": filename,
                "filepath": file_path
            }
            return
        # 加载文件
        filetype = self.get_filetype(file_path)
        cls = self.mk_builder(filetype)
        try:
            for row in cls(file_path, **self.extra_args)():
                yield {
                    "filename": filename,
                    "data": row
                }
        except:
            print("Error occur when opening file:", file_path)
            traceback.print_exc()

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

