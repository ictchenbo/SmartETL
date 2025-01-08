import os
import json
from .files import get_lines


def from_text(file: str, encoding="utf8"):
    """基于文本文件构造set"""
    s = set()
    if not os.path.exists(file):
        print('Warning: file not exists:', file)
        return s
    for line in get_lines(file, encoding=encoding):
        s.add(line.strip())
    return s


def from_csv(file: str, key_col=0, encoding="utf8"):
    """基于csv文件构造set"""
    s = set()
    if not os.path.exists(file):
        print('Warning: file not exists:', file)
        return s
    for line in get_lines(file, encoding=encoding):
        if "," in line:
            s.add(line.split(",")[key_col])
    return s


def from_json(file: str, key_key='id', encoding="utf8"):
    """基于json文件构造set"""
    s = set()
    if not os.path.exists(file):
        print('Warning: file not exists:', file)
        return s
    for line in get_lines(file, encoding=encoding):
        row = json.loads(line)
        if key_key in row:
            s.add(row[key_key])
    return s
