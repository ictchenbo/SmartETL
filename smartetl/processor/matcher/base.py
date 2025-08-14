from smartetl.processor.filter import Filter
from smartetl.util.jsons import extract


class MatchBase(Filter):
    """
    基于JSON数据的规则匹配
    """
    def __init__(self):
        super().__init__(self)
    
    def match(self, row: dict) -> bool:
        return True

    def __call__(self, *args, **kwargs):
        return self.match(args[0])


class SimpleMatch(MatchBase):
    """基于给定的字典参数进行逐字段匹配"""
    def __init__(self, reference: dict = None, **kwargs):
        super().__init__()
        self.match_rules = reference or {}
        self.match_rules.update(kwargs)

    def match(self, row: dict) -> bool:
        for key, value in self.match_rules.items():
            if key not in row or extract(row, key) != value:
                return False
        return True


class JsonPathMatch(MatchBase):
    """
    基于jsonpath的匹配器
    """
    def __init__(self, pattern: str):
        super().__init__()
        self.pattern = pattern
        from jsonpath2.path import Path
        self.p = Path.parse_str(pattern)

    def match(self, row: dict) -> bool:
        for _ in self.p.match(row):
            return True
        return False
