import requests
import json


api_base = 'http://10.170.130.21:9500/kg/v1.0'
# api_base = 'http://localhost:9500/kg/v1.0'


def triplets2dict(filename: str):
    """将三元组数据转为属性图数据（实体）"""
    last_name = None
    last_obj = {}
    with open(filename, encoding='utf8') as fin:
        for line in fin:
            line = line.strip()
            pos = line.find(',')
            name = line[:pos].strip()
            line = line[pos+1:].strip()
            pos = line.find(',')
            prop = line[:pos].strip()
            value = line[pos+1:].strip()
            # name, prop, value = values
            if name != last_name:
                if last_name:
                    yield last_obj
                last_name = name
                last_obj = {
                    "_name": name
                }
            if prop not in last_obj:
                last_obj[prop] = value
            elif not isinstance(last_obj[prop], list):
                last_obj[prop] = [last_obj[prop]]
                last_obj[prop].append(value)
            else:
                last_obj[prop].append(value)
    if last_name:
        yield last_obj


def dict2entity(row: dict, name_key: str = 'name', types_key: str = 'type'):
    """dict转为KGMS的实体结构"""
    node = {}
    props = {}
    if name_key in row:
        node['name'] = row.pop(name_key)
    if types_key in row:
        val = row.pop(types_key)
        node['types'] = val if isinstance(val, list) else [val]
    for k, v in row.items():
        if k in ["name"]:
            continue
        props[k] = {
            "values": v
        }
    node["properties"] = props
    return node


def search_entity_type(name: str, ontology_tree: dict or list):
    """遍历本体树获得实体类型的ID"""
    if isinstance(ontology_tree, dict):
        if ontology_tree.get("name") == name:
            return ontology_tree.get("_id")
        return search_entity_type(name, ontology_tree.get("children", []))
    else:
        for node in ontology_tree:
            if node.get("name") == name:
                return node.get("_id")
            val = search_entity_type(name, node.get("children", []))
            if val:
                return val
        return None


def search_entity(name: str, graph_id: str = 'default', cache: dict = None):
    """检索KGMS实体 支持本地缓存"""
    if cache and name in cache:
        return cache[name]
    url = f'{api_base}/{graph_id}/graph/entity/_search'
    res = requests.post(url, json={'name': name})

    # print(res.text)
    if res.status_code == 200:
        data = res.json()['result']['data']
        if data:
            _id = data[0]['_id']
            if cache:
                cache[name] = _id
            return _id
    else:
        print("ERROR search_entity: ", res.text)
    return None


def insert_entities(entities: dict or list, graph_id: str = 'default'):
    """写入KGMS实体"""
    url = f'{api_base}/{graph_id}/graph/entity'
    res = requests.post(url, json=entities)
    if res.status_code == 200:
        result = res.json()['result']
        print('inserted: ', result['inserted_count'])
        return result['inserted']
    print("ERROR insert_entities: ", res.text)
    return None


def insert_relations(relations: dict or list, graph_id: str = 'default'):
    """写入KGMS实体关系"""
    url = f'{api_base}/{graph_id}/graph/relation'
    res = requests.post(url, json=relations)
    if res.status_code == 200:
        result = res.json()['result']
        print('inserted: ', result['inserted_count'])
        return result['inserted']
    print("ERROR insert_relations: ", relations, res.text)
    return False


if __name__ == '__main__':
    print(search_entity('卫星星座', graph_id='123'))
