from smartetl.iterator.base import JsonIterator, DictProcessorBase
from smartetl.util.jsons import extract, fill


class Select(DictProcessorBase):
    """
    Select操作 key支持嵌套，如`user.name`表示user字段下面的name字段 并将name作为结果字段名
    """
    def __init__(self, *keys, short_key: bool = False):
        assert len(keys) > 0, "必须指定一个或多个字段名称"
        if isinstance(keys[0], list) or isinstance(keys[0], tuple):
            self.keys = keys[0]
        else:
            self.keys = keys
        self.short_key = short_key
        self.path = {}
        for key in self.keys:
            path = key.split('.')
            if short_key:
                key = path[-1]
            self.path[key] = path

    def on_data(self, data: dict, *args):
        return {key: extract(data, path) for key, path in self.path.items()}

    def __str__(self):
        return f"{self.name}(keys={self.keys}, short_key={self.short_key})"


class SelectVal(DictProcessorBase):
    """
    字段值选择操作 指定字段key的值作为新的数据返回
    """
    def __init__(self, key: str, inherit_props: bool = False):
        self.key = key
        self.inherit_props = inherit_props

    def on_data(self, data: dict, *args):
        keyval = extract(data, self.key)
        if self.inherit_props:
            if isinstance(keyval, dict):
                for k, v in data.items():
                    if k != self.key:
                        keyval[k] = v
            else:
                print("SelectVal Warning: field value must be dict when inherit_props is True")
        return keyval

    def __str__(self):
        return f"{self.name}('{self.key}', inherit_props={self.inherit_props})"


class RemoveFields(DictProcessorBase):
    """
    移除部分字段
    """
    def __init__(self, key: str or list or tuple, *keys):
        super().__init__()
        if isinstance(key, list) or isinstance(key, tuple):
            self.keys = list(key)
        else:
            self.keys = [key]
        self.keys.extend(keys)

    def on_data(self, data: dict, *args):
        for k in self.keys:
            if k in data:
                del data[k]
        return data
        # return {k: v for k, v in data.items() if k not in self.keys}

    def __str__(self):
        return f"{self.name}(keys={self.keys})"


def is_empty_or_null(v):
    if isinstance(v, int) or isinstance(v, float):
        return False
    return not v


class RemoveEmptyOrNullFields(JsonIterator):
    """移除空的字段或元素 对于dict/list/tuple数据有效 其他类型原样返回"""
    def on_data(self, data, *args):
        if isinstance(data, dict):
            return {k: v for k, v in data.items() if not is_empty_or_null(v)}
        if isinstance(data, list):
            return [v for v in data if not is_empty_or_null(v)]
        if isinstance(data, tuple):
            return tuple([v for v in data if not is_empty_or_null()])
        return data


class DictEditBase(DictProcessorBase):
    templates: dict = {}

    def __init__(self, tmp: dict = None, **kwargs):
        self.templates = tmp or {}
        self.templates.update(kwargs)

    def __str__(self):
        return f"{self.name}(**{self.templates})"


class AddFields(DictEditBase):
    """添加字段 如果字段不存在"""
    def __init__(self, tmp: dict = None, **kwargs):
        super().__init__(tmp=tmp, **kwargs)

    def on_data(self, data: dict, *args):
        for k, v in self.templates.items():
            if k not in data:
                data[k] = v
        return data


class ReplaceFields(DictEditBase):
    """替换字段 Upsert模式"""
    def __init__(self, tmp: dict = None, **kwargs):
        super().__init__(tmp=tmp, **kwargs)

    def on_data(self, data: dict, *args):
        for k, v in self.templates.items():
            data[k] = v
        return data


class RenameFields(DictEditBase):
    """对字段重命名 如果目标字段存在则会被覆盖"""
    def __init__(self, tmp: dict = None, **kwargs):
        super().__init__(tmp=tmp, **kwargs)

    def on_data(self, data: dict, *args):
        for s, t in self.templates.items():
            if s in data:
                data[t] = data.pop(s)
        return data


class MergeFields(DictEditBase):
    """合并字段，如果某字段不存在或值为空，使用指定字段进行填充"""
    def __init__(self, tmp: dict = None, **kwargs):
        super().__init__(tmp=tmp, **kwargs)

    def on_data(self, data: dict, *args):
        for s, t in self.templates.items():
            data[t] = data.get(t) or data.get(s)
        return data


class CopyFields(DictEditBase):
    """复制已有的字段 如果目标字段名存在 则覆盖"""
    def __init__(self, tmp: dict = None, **kwargs):
        super().__init__(tmp=tmp, **kwargs)

    def on_data(self, data: dict, *args):
        for s, t in self.templates.items():
            data[t] = data.get(s)
        return data


class InjectField(DictProcessorBase):
    """
    基于给定的KV缓存对当前数据进行填充
    """
    def __init__(self, kv: dict, inject_path: str or list, reference_path: str = None):
        assert kv, "kv should not be empty"
        assert inject_path, "inject_path should not be empty"
        self.kv = kv
        self.inject_path = inject_path
        self.reference_path = reference_path or inject_path

    def on_data(self, item: dict, *args):
        match_val = extract(item, self.reference_path)
        if match_val and match_val in self.kv:
            val = self.kv[match_val]
            fill(item, self.inject_path, val)
        return item


class ConcatFields(DictProcessorBase):
    """连接数个已有的字段值，形成新字段。如果目标字段名存在 则覆盖；如果只有一个来源字段，与CopyFields效果相同"""
    def __init__(self, target_key: str, *source_keys, separator: str = '_', prefix: str = '', suffix: str = ''):
        super().__init__()
        self.target_key = target_key
        self.source_keys = source_keys
        self.separator = separator if separator is not None else '_'
        self.prefix = prefix
        self.suffix = suffix

    def on_data(self, data: dict, *args):
        vals = [str(data.get(k)) for k in self.source_keys]
        data[self.target_key] = f'{self.prefix}{self.separator.join(vals)}{self.suffix}'
        return data


class ConcatArray(DictProcessorBase):
    """拼接数组字段，形成新字段"""
    def __init__(self, *source_keys, target_key: str = None):
        super().__init__()
        self.source_keys = source_keys
        self.target_key = target_key

    def on_data(self, data: dict, *args):
        res = []
        for k in self.source_keys:
            if k in data:
                val = data[k]
                if isinstance(val, list) or isinstance(val, tuple) or isinstance(val, set):
                    res.extend(val)
                else:
                    res.append(val)
        if self.target_key:
            data[self.target_key] = res
            return data
        return res
