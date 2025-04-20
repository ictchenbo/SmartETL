from .dates import date2ts


methods = {
    "lower": lambda s: s.lower(),
    "upper": lambda s: s.upper(),
    "strip": lambda s: s.strip(),
    "int": int,
    "float": float,
    "toDate": date2ts
}


def format(val: str, *args, **kwargs):
    """对指定字段（为模板字符串）使用指定的值进行填充"""
    return val.format(**kwargs)


def f(method: str):
    assert method in methods, "method not supported"
    return methods[method]


def splicing_characters(string_data: str):
    import time
    time.sleep(7)
    return f"https://arxiv.org/html/{string_data}"


def splicing_characters_pdf(string_data: str):
    import time
    time.sleep(7)
    return f"https://arxiv.org/pdf/{string_data}"
