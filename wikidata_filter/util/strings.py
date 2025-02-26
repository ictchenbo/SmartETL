from .dates import date2ts


methods = {
    "lower": lambda s: s.lower(),
    "upper": lambda s: s.upper(),
    "strip": lambda s: s.strip(),
    "int": int,
    "float": float,
    "toDate": date2ts
}


def f(method: str):
    assert method in methods, "method not supported"
    return methods[method]
