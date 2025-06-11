import os
import io
import json as JSON


def content(filename: str):
    """读取文件字节内容"""
    with open(filename, "rb") as fin:
        return fin.read()


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
    """打开文件 返回文件流 根据文件名判断是否为gz bz2 xz压缩文件或普通文件"""
    # 首先判断是否为字节流
    if isinstance(filename, io.IOBase):
        if 'b' in mode:
            return filename
        # 包装为文本流
        return io.TextIOWrapper(filename, encoding=encoding)
    # 判断是否为字节数据
    if isinstance(filename, bytes):
        stream = io.BytesIO(filename)
        if 'b' in mode:
            return stream
        return io.TextIOWrapper(stream, encoding=encoding)

    if filename.endswith('.gz'):
        import gzip
        stream = gzip.open(filename, mode, **kwargs)
    elif filename.endswith('.bz2'):
        import bz2
        stream = bz2.open(filename, mode, encoding=encoding, **kwargs)
    elif filename.endswith('.xz'):
        import lzma
        stream = lzma.open(filename, mode, encoding=encoding, **kwargs)
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
