# 文件加载器统一管理

from smartetl.base import ROOT, LOADER_MODULE
from smartetl.util.mod_util import load_cls
from .file import BinaryFile
from .text import Text, CSV, Json, JsonLine, JsonArray, JsonFree, Yaml, TextPlain


# 简单文件类型 直接加载
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


# 复杂文件类型注册在这里，根据需要动态加载对应模块
ALL_LOADERS = {
    '.raw': 'file.BinaryFile',
    '.doc': 'doc.Doc',
    '.docx': 'doc.Docx',
    '.ppt': 'doc.PPT',
    '.pptx': 'doc.PPTX',
    '.pdf': 'pdf.PDF',
    '.xls': 'xls.ExcelStream',
    '.xlsx': 'xls.ExcelStream',
    '.parquet': 'parquet.Parquet',
    '.eml': 'eml.EML',
    '.pst': 'pst.PST',
    '.ost': 'pst.OST',
    '.zip': 'ar.Zip',
    '.rar': 'ar.Rar',
    '.tar': 'ar.Tar',
    '.7z': 'ar.SevenZip'
}


def get_file_loader(_type: str, *args, **kwargs):
    """根据文件类型获取对应的文件加载器"""
    assert _type in LOADERS or _type in ALL_LOADERS, f"{_type} file not supported"
    if _type in LOADERS:
        return LOADERS[_type]
    loader = load_cls(f'{ROOT}.{LOADER_MODULE}.{ALL_LOADERS[_type]}')[0]
    LOADERS[_type] = loader
    return loader
