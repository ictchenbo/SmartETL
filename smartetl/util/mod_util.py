from importlib import import_module
from smartetl.base import ROOT, TOP_MODULES


def load_module(pkg: str):
    """根据模块名加载模块对象"""
    try:
        mod = import_module(pkg)
        # mod = __import__(pkg, fromlist=pkg)
    except ImportError:
        raise Exception(f"module [{pkg}] not found!")
    return mod


def load_cls(full_name: str):
    """根据对象全名加载对象"""
    pkg = full_name[:full_name.rfind('.')]
    class_name_only = full_name[full_name.rfind('.') + 1:]
    mod = load_module(pkg)

    try:
        cls = mod.__dict__[class_name_only]
        # cls = eval(class_name_only, globals(), mod.__dict__)
    except AttributeError:
        raise Exception(f"class [{class_name_only}] not found in module [{pkg}]!")

    return cls, mod, class_name_only


def parse_args(expr: str):
    import ast

    tree = ast.parse(expr, mode='eval')

    if isinstance(tree.body, ast.Call):
        args = [ast.dump(arg) for arg in tree.body.args]  # 获取位置参数
        kwargs = {kw.arg: ast.dump(kw.value) for kw in tree.body.keywords}  # 获取关键字参数
        return args, kwargs
    else:
        return None


def load_util(mod: str):
    """加载工具类对象"""
    if mod is not None and isinstance(mod, str):
        # 兼容V2中database模块位置
        if mod.startswith('util.database.'):
            mod = mod[5:]
        for top in TOP_MODULES:
            if mod.startswith(f'{top}.'):
                mod = f'{ROOT}.{mod}'
                break

        return load_cls(mod)[0]

    return mod
