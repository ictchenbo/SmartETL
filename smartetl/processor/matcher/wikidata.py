"""
从下载的wikidata数据中筛选出指定类型的实体

输入：指定实体类型，支持多个（如Q5）
输出：将匹配的实体（Item）输出

处理过程：检查Item条目的声明（claims） 看其P31是否关联到指定的实体类型

详细设计：
1. 数据按行读取 加载为JSON （注意原始wikidata JSON第一行为[ 最后一行为 ] 其他行末尾有一个逗号）
2. 判断 `type` 是否为 `item` 如果不是则跳过
3. 读取 `claims`的 `P31`声明  如果没有该声明则跳过
4. 遍历`P31`声明 查看其 `mainsnack.datatype`属性 如果不为`wikibase-item` 则跳过
5. 查看 `mainsnack.datavalue.value.id` 是否在给定的实体类型列表中
6. 将满足条件的行输出到指定文件中
"""

from smartetl.processor.matcher.base import MatchBase
from smartetl.util.jsons import extract


class WikidataMatch(MatchBase):
    """
    基于属性的匹配，配置示例：
        {"P31": ["Q5", "Qxx"]}   ->  具有P31属性 且值在给定范围内
        {"P580": True}    ->  具有P580属性 不限定值
        {"P39": False}    ->  要求不具有P39属性
    基于原始wikidata结构的匹配 采用.claims[Pxx].mainsnak
    """
    def __init__(self, reference: dict = None, **match_relations):
        super().__init__()
        val = reference or {}
        val.update(match_relations)
        self.match_relations = val

    def match(self, item: dict) -> bool:
        # if item["type"] != "item":
        #     return False
        claims = item["claims"]

        for k, targets in self.match_relations.items():
            if isinstance(targets, bool):
                if targets and k not in claims:
                    return False
                if not targets and k in claims:
                    return False
                continue
            if not targets:
                continue
            if k not in claims:
                return False
            if not self.match_object(claims[k], targets):
                return False

        return True

    def match_object(self, claims: list[dict], targets: list[str]) -> bool:
        object_set = self.extract_objects(claims)
        for target in targets:
            if target in object_set:
                return True
        return False

    def extract_objects(self, claims: list[dict]) -> set:
        object_set = set()
        for claim in claims:
            mainsnak = claim.get('mainsnak')
            if mainsnak and mainsnak.get('datatype') == "wikibase-item":
                object_id = extract(mainsnak, ['datavalue', 'value', 'id'])
                if object_id is not None:
                    object_set.add(object_id)
        return object_set


class WikidataMatchNew(WikidataMatch):
    """基于处理后的wikidata结构的匹配 采用datavalue集合"""
    def __init__(self, reference: dict = None, **match_relations):
        super().__init__(reference, **match_relations)

    def extract_objects(self, claims: list[dict]) -> set:
        return {val.get('datavalue') for val in claims if 'datavalue' in val and val.get('datatype') == 'wikibase-entityid'}
