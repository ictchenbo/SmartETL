import os
import json as JSON


def text(filename: str, encoding="utf8", **kwargs):
    """读取文本文件"""
    with open(filename, encoding=encoding, **kwargs) as fin:
        return fin.read()


def json(filename: str, encoding="utf8", **kwargs):
    """读取JSON"""
    with open(filename, encoding=encoding, **kwargs) as fin:
        return JSON.load(fin)


def json_lines(filename: str, encoding="utf8", **kwargs):
    """读取每行并加载为json"""
    for line in get_lines(filename, encoding=encoding):
        yield JSON.loads(line)


def get_lines(filename: str, encoding="utf8", **kwargs):
    """读取每行并作为文本返回"""
    with open(filename, "r", encoding=encoding, **kwargs) as fin:
        for line in fin:
            yield line.strip()


def open_file(filename: str, mode: str = "rb", encoding: str = "utf8", **kwargs):
    """打开文件 返回文件流 根据文件名判断是否为bz2 gz或普通文件"""
    if filename.endswith('.bz2'):
        import bz2
        stream = bz2.open(filename, mode, encoding=encoding, **kwargs)
    elif filename.endswith('.gz'):
        import gzip
        stream = gzip.open(filename, mode, **kwargs)
    else:
        stream = open(filename, mode, encoding=encoding, **kwargs)
    return stream


def display_file_content(filename: str, encoding="utf8", limit=1000):
    with open_file(filename) as fin:
        for line in fin:
            print(line.decode(encoding))
            limit -= 1
            if limit <= 0:
                break


def exists(filename: str) -> bool:
    """判断文件是否存在"""
    return os.path.exists(filename)
