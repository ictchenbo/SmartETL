import os
import io
import json as JSON
import traceback


def filetype(file_path: str, type_mapping: dict):
    """根据文件路径和指定的类型映射规则，获取文件类型"""
    if 'all' in type_mapping:
        return type_mapping['all']
    filename = os.path.split(file_path)[1]
    if '.' not in filename:
        return ''
    suffix = filename[filename.rfind('.'):]
    if suffix in type_mapping:
        return type_mapping[suffix]
    if 'other' in type_mapping:
        return type_mapping['other']
    return suffix


def content(filename: str) -> bytes:
    """读取文件字节内容"""
    with open(filename, "rb") as fin:
        return fin.read()


def text(filename: str, encoding="utf8", **kwargs) -> str:
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


def read(filename: str, loader: str = "auto", type_mapping: dict = None, **extra_args):
    """利用指定的文件加载器读取文件内容 如果loader为'auto'，则根据文件名后缀自动判断加载器类型"""
    if loader == 'auto':
        loader = filetype(filename, type_mapping or {})
    from smartetl.loader.file_loaders import get_file_loader
    cls = get_file_loader(loader)
    try:
        for row in cls(filename, **extra_args)():
            return row
    except:
        print("Error occur when opening file:", filename)
        traceback.print_exc()
    
    return None


def get_lines(filename: str, encoding="utf8", **kwargs):
    """读取每行并作为文本返回"""
    # for line in open_file(filename, **kwargs):
    #     yield line.decode(encoding)
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


def write_json(filename: str, data, encoding="utf8"):
    """输出JSON文件"""
    with open(filename, "w", encoding=encoding) as fout:
        JSON.dump(data, fout, ensure_ascii=False)


def write_json_lines(filename: str, data, encoding="utf8"):
    """输出JSON行文件"""
    with open(filename, "w", encoding=encoding) as fout:
        for record in data:
            fout.write(JSON.dumps(record, ensure_ascii=False))
            fout.write('\n')


def exists(filename: str) -> bool:
    """判断文件是否存在"""
    return os.path.exists(filename)


def basename(filename: str) -> str:
    """获取文件路径的文件名"""
    return os.path.basename(filename)


def dirname(filename: str) -> str:
    """获取文件路径的文件夹名"""
    return os.path.dirname(filename)


def filesize(filename: str) -> int:
    """获取文件大小 返回文件字节数"""
    return os.stat(filename).st_size


def mkdirs(path: str) -> str:
    """创建指定路径的文件夹 包括中间文件夹，如果文件夹已存在也不会报错"""
    os.makedirs(path, exist_ok=True)
    return path


if __name__ == '__main__':
    print(filesize('files.py'))
