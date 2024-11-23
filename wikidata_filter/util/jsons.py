def split_path(path: list or str):
    if isinstance(path, str):
        path = path.split('.')
    return path


def extract(val: dict, path: list or str):
    """获取json的某个字段 支持嵌套字段获取"""
    if isinstance(path, str) and '.' not in path:
        return val[path]
    path = split_path(path)
    for key in path:
        if key not in val:
            return None
        val = val[key]
    return val


def extract_num(val: dict, path: str):
    """提取json中的数值字段 返回（num_val, ok）"""
    val = val.get(path)
    if val is None:
        return val, False
    if isinstance(val, int) or isinstance(val, float):
        return val, True
    val = str(val).replace(',', '')  # 去掉千分位逗号
    try:
        return float(val), True
    except:
        print('Warning: value not number: ', val)
        return None, False


def get_valid(items: list, key: str):
    """获取json列表中的有效的数值列表"""
    res = []
    for item in items:
        val, ok = extract_num(item, key)
        if ok:
            res.append(val)
    return res


def fill(target: dict, path: list or str, value):
    """设置json的某个字段 支持嵌套字段设置"""
    path = split_path(path)
    for part in path[:-1]:
        if part not in target:
            target = target[part] = {}
        target = target[part]
    target[path[-1]] = value
