from wikidata_filter.iterator.base import DictProcessorBase
from wikidata_filter.iterator.field_based import DictEditBase
from wikidata_filter.util.langs import zh_simple

name_keys = ["zh-cn", "zh-hans", "zh-sg", "zh-hk", "zh-tw", "zh-mo", "en"]
name_en_keys = ["en"]
wiki_sites = ["zhwiki", "enwiki"]


def get_label(v):
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        return v["value"]
    return [vi["value"] if isinstance(vi, dict) else vi for vi in v]


def get_name(labels, keys, default=None):
    if labels is None:
        return default
    # 兼容非dict
    if not isinstance(labels, dict):
        return labels
    for key in keys:
        if key in labels:
            return get_label(labels[key])
    for key in labels.keys():
        return get_label(labels[key])

    return default


def get_props(item):
    qid = item["id"]
    claims = item.get("claims", {})
    # print(claims)
    for pid in claims.keys():
        claim_items = claims[pid]
        if isinstance(claim_items, dict):
            claim_items = [claim_items]
        for claim in claim_items:
            _type, _value = get_snak(claim.get("mainsnak"))
            if not _type or not _value:
                continue
            prop = {
                "id": claim["id"],  # 属性claimID
                "qid": qid,  # item ID
                "pid": pid,  # 属性ID
                "datatype": _type,
                "datavalue": _value
            }
            prop_props = list(get_qualifiers(claim, pid))
            yield prop, prop_props


def get_qualifiers(claim, pid):
    """
    获取属性的属性
    :param claim: 属性声明
    :param pid: 属性id
    :return:
    """
    cid = claim["id"]
    qs = claim.get("qualifiers", {})
    for pid2, snaks in qs.items():
        for snak in snaks:
            _type, _value = get_snak(snak)
            if not _type or not _value:
                continue
            yield {
                "id": cid,  # 属性claimID
                "hash": snak["hash"],  # 属性属性的HASH值
                "pid": pid,  # 属性ID
                "ppid": pid2,  # 属性属性ID
                "datatype": _type,
                "datavalue": _value
            }


def get_snak(snak):
    """
    snak中提取数据类型和数据值
    :param snak:
    :return:
    """
    _type = snak["snaktype"]

    if _type == "value" and "datavalue" in snak:
        datavalue = snak["datavalue"]
        vt, _value = datavalue["type"], datavalue["value"]
        if vt == "wikibase-entityid":
            _value = _value["id"]
        # elif vt == "globecoordinate":
        #     _value = [_value["latitude"], _value["longitude"]]
        # elif vt == "quantity":
        #     _value = _value["amount"]
        # elif vt == "time":
        #     _value = _value["time"]
        # elif vt == "monolingualtext":
        #     _value = f'{_value["language"]}:{_value["text"]}'

        return vt, _value

    return _type, None


class IDNameMap(DictProcessorBase):
    """
    建立ID到name的映射
    """
    def on_data(self, data: dict, *args):
        return {
            "id": data["id"],
            "name": get_name(data.get('labels'), name_keys)
        }


class Simplify(DictProcessorBase):
    """
    对wikidata对象进行简化
    {id: str, labels: str, descriptions: str, aliases: list[str], claims: dict[str, list], sitelinks}
    """
    del_keys = ['type', 'title', 'ns', 'pageid']

    def on_data(self, item: dict, *args):
        for key in self.del_keys:
            if key in item:
                del item[key]
        # 支持重入
        item['labels'] = get_name(item.get('labels'), name_keys)
        item['descriptions'] = get_name(item.get('descriptions'), name_keys)
        item['aliases'] = get_name(item.get('aliases'), name_keys)

        # sitelink 最多保留一个
        if 'sitelinks' in item:
            sitelinks = item.pop('sitelinks')
            for site in wiki_sites:
                if site in sitelinks:
                    item['sitelinks'] = sitelinks[site]
                    break

        return item


class SimplifyProps(DictProcessorBase):
    """
    对原wikidata对象属性(claims)进行简化
    claims := dict[str, list[Prop]]
    Prop := {id: str, datatype: str, datavalue: PropVal,  qualifiers: dict[str, list[QProp]}
    QProp := {hash: str, datatype: PropDataType, datavalue: PropVal }
    PropDataType :=  wikibase-entityid | globecoordinate | quantity | time | monolingualtext | novalue | somevalue
    """
    def on_data(self, item: dict, *args):
        item['claims'] = list(get_props(item))
        return item


class PropsFilter(DictProcessorBase):
    """
    对Item属性按照属性Key进行过滤
    """
    def __init__(self, props_set: set = None, props_set_file: str = None):
        super().__init__()
        props_set = props_set or set()
        if props_set_file:
            from wikidata_filter.util.sets import from_csv
            props_set.update(from_csv(props_set_file))
        self.props_set = props_set

    def on_data(self, item: dict, *args):
        new_claims = {}
        for key, vals in item.get("claims", {}).items():
            if key in self.props_set:
                new_claims[key] = vals

        item['claims'] = new_claims

        return item


class ValuesFilter(PropsFilter):
    """
    对Item属性按照属性Value进行过滤
    """
    def __init__(self, props_set: set = None, props_set_file: str = None):
        super().__init__(props_set=props_set, props_set_file=props_set_file)

    def __call__(self, item: dict):
        new_claims = {}
        for key, vals in item.get("claims", {}).items():
            new_vals = [val for val in vals if val.get("datatype") == "wikibase-entityid" and val["datavalue"] in self.props_set]
            if new_vals:
                new_claims[key] = new_vals

        if len(new_claims) == 0:
            return None

        item['claims'] = new_claims
        return item


class ObjectNameInject(DictEditBase):
    """
    基于给定的KV缓存 对当前实体的属性值实体增加其名称
    """
    FILL_KEY = 'labels'

    def __init__(self, cache: dict = None, **kwargs):
        super().__init__(tmp=cache, **kwargs)

    def fill_val(self, target: dict):
        if target["datatype"] in ["item", "wikibase-entityid"] and target.get("datavalue") in self.templates:
            ref_key = target.get("datavalue")
            # print('filled', ref_key)
            target[self.FILL_KEY] = self.templates[ref_key]

    def on_data(self, item: dict, *args):
        for prop, claims in item.get('claims', {}).items():
            for claim in claims:
                self.fill_val(claim)
                for sub_prop, sub_claims in claim.get('qualifiers', {}).items():
                    for sub_claim in sub_claims:
                        self.fill_val(sub_claim)

        return item


class ItemAbstractInject(DictEditBase):
    """
    基于给定的KV缓存查找并填充实体的摘要信息
    kv: {'enwiki': {'Alfred Hitchcoc': 'Master of Suspense (album)' }, 'zhwiki': {} }
    """
    def __init__(self, cache: dict, target_key: str = 'abstract', source_key: str = 'sitelinks'):
        super().__init__(tmp=cache)
        self.source_key = source_key
        self.target_key = target_key

    def on_data(self, item: dict, *args):
        if self.source_key in item:
            site = item[self.source_key]
            sitename = site["site"]
            pagetitle = site["title"]

            if sitename in self.templates:
                site_data = self.templates[sitename]
                if pagetitle in site_data:
                    item[self.target_key] = site_data[pagetitle]

        return item


class ChineseSimple(DictProcessorBase):
    """"对item指定字段以及其属性值的labels进行繁简体转换"""
    def __init__(self, keys: list = ['labels', 'descriptions', 'abstract']):
        self.keys = keys

    def on_data(self, item: dict, *args):
        for key in self.keys:
            if key in item and item[key]:
                item[key] = zh_simple(item[key])

        for prop, claims in item.get('claims', {}).items():
            for claim in claims:
                if claim.get('labels'):
                    claim['labels'] = zh_simple(claim['labels'])

        return item


class Entity(DictProcessorBase):
    """
    输出wikidata item/property基本信息
    """
    def on_data(self, entity: dict, *args):
        if "labels" not in entity or "descriptions" not in entity:
            return None
        eid = entity["id"]
        return {
            "_id": int(eid[1:]),
            "_type": entity.get("type") or "item",
            "id": eid,
            # "modified": entity["modified"],
            "name": get_name(entity.get("labels"), name_keys),
            "name_en": get_name(entity.get("labels"), name_en_keys),
            "desc": get_name(entity.get("descriptions"), name_keys),
            "desc_en": get_name(entity.get("descriptions"), name_en_keys),
            "aliases": get_name(entity.get("aliases"), name_keys)
        }


class Property(DictProcessorBase):
    """
    输出wikidata的实体属性和属性属性
    """
    def on_data(self, entity: dict, *args):
        if entity["type"] != "item":
            return None
        for prop, prop_props in get_props(entity):
            prop["_type"] = "item_property"
            yield prop
            for prop_prop in prop_props:
                prop_prop["_type"] = "property_property"
                yield prop_prop


class Relation(DictProcessorBase):
    """
    基于item的claims生成关系结构
    """
    def on_data(self, item: dict, *args):
        subject_id = item["id"]
        subject_name = item.get("labels")
        claims = item.get('claims', {})
        for relation, objects in claims.items():
            for obj in objects:
                yield {
                    "_id": obj["id"],
                    "subject_id": subject_id,
                    "subject_name": subject_name,
                    "relation": relation,
                    "object_id": obj["datavalue"],
                    "object_name": obj.get("labels")
                }
