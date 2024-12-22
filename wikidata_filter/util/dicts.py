import json
from wikidata_filter.util.files import get_lines


def from_csv(file, key_col=0, val_col=1, sep=",", encoding="utf8"):
    kv = {}
    for line in get_lines(file, encoding=encoding):
        if sep in line:
            parts = line.split(sep)
            kv[parts[key_col].strip()] = parts[val_col]
    return kv


def from_json(file, key_key='id', val_key='name', encoding="utf8"):
    kv = {}
    for line in get_lines(file, encoding=encoding):
        row = json.loads(line)
        if key_key in row and val_key in row:
            kv[row[key_key]] = row[val_key]
    return kv


def copy_val(val):
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


def reverse_dict(source: dict):
    return {v: k for k, v in source.items()}
