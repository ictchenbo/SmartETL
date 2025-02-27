import json
from wikidata_filter.util.files import get_lines


def from_csv(file: str, key_col: int = 0, val_col: int = 1, sep: str = ",", encoding: str = "utf8"):
    """基于csv构造dict
    :param file 输入文件
    :param key_col key字段序号，默认0第一列
    :param val_col value字段序号，默认1第二列
    :param sep 分隔符，默认半角逗号
    :param encoding 文件编码，默认为utf8
    """
    kv = {}
    for line in get_lines(file, encoding=encoding):
        if sep in line:
            parts = line.split(sep)
            kv[parts[key_col].strip()] = parts[val_col]
    return kv


def from_json(file: str, key_key: str = 'id', val_key: str = 'name', encoding: str = "utf8"):
    """基于json构造dict
        :param file 输入文件
        :param key_key key字段名，默认为'id'
        :param val_key value字段名，默认为'name'
        :param encoding 文件编码，默认为utf8
    """
    kv = {}
    for line in get_lines(file, encoding=encoding):
        row = json.loads(line)
        if key_key in row and val_key in row:
            kv[row[key_key]] = row[val_key]
    return kv


def copy_val(val):
    """Python基本对象值拷贝（深拷贝），但不支持复杂对象"""
    if isinstance(val, dict):
        return {k: copy_val(v) for k, v in val.items()}
    elif isinstance(val, set):
        return {k for k in val}
    elif isinstance(val, list):
        return [copy_val(v) for v in val]
    elif isinstance(val, tuple):
        return tuple([copy_val(v) for v in val])
    else:
        return val


def merge_dicts(target: dict, source: dict):
    """将source字典合并到target中，相同字段会替换"""
    for k, v in source.items():
        if k in target and isinstance(target[k], dict):
            # 字典对象递归合并
            merge_dicts(target[k], v)
        else:
            target[k] = copy_val(v)


def reverse(source: dict):
    """反转dict的k-v"""
    return {v: k for k, v in source.items()}
