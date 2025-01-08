from importlib import import_module


def load_module(pkg: str):
    try:
        mod = import_module(pkg)
        # mod = __import__(pkg, fromlist=pkg)
    except ImportError:
        raise Exception(f"module [{pkg}] not found!")
    return mod


def load_cls(full_name):
    # print('load_cls:', full_name)
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
