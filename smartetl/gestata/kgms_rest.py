import requests


API_DEFAULT = 'http://10.170.130.21:9500/kg/v1.0'
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


def group2dict(triples: list, subject_key='h', object_key='t', predicate_key='r'):
    """分组的三元组列表转换为一个实体KV对象"""
    entity = {}
    for triple in triples:
        key = triple.get(predicate_key)
        value = triple.get(object_key)
        if not key or not value:
            continue
        if key not in entity:
            entity[key] = value
        else:
            vals = entity[key]
            if isinstance(vals, list):
                vals.append(value)
            else:
                vals = [vals, value]
            entity[key] = vals
    return entity


def dict2entity(row: dict, name_key: str = 'name', types_key: str = 'type', props_format: str = 'nested'):
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
        props[k] = {"values": v} if props_format == 'nested' else [v]
    node["properties"] = props
    return node


def wrap_property(properties: dict):
    new_props = {}
    for k, v in properties.items():
        new_props[k] = {"values": v}
    return new_props


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


def search_entity(name: str, graph_id: str = 'default', cache: dict = None, api_base: str = API_DEFAULT):
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


def insert(url: str, json, name: str = 'entity'):
    res = requests.post(url, json=json)
    if res.status_code == 200:
        result = res.json()['result']
        print('inserted: ', result)
        if isinstance(json, list):
            if len(json) != len(result['inserted']):
                print(result['failed'])
        else:
            if not result['inserted']:
                print(result['failed'])
        return result['inserted']
    print(f"ERROR to insert {name}: ", res.status_code, res.text)
    return None


def insert_entities(entities: dict or list, graph_id: str = 'default', api_base: str = API_DEFAULT):
    """写入KGMS实体"""
    return insert(f'{api_base}/{graph_id}/graph/entity', entities, name='entity')


def insert_relations(relations: dict or list, graph_id: str = 'default', api_base: str = API_DEFAULT):
    """写入KGMS实体关系"""
    return insert(f'{api_base}/{graph_id}/graph/relation', relations, name='relation')


def insert_graph(objects: dict or list, object_type: str = 'entity', graph_id: str = 'default', api_base: str = API_DEFAULT):
    """新增实体或关系"""
    return insert(f'{api_base}/{graph_id}/graph/{object_type}', objects, name=object_type)


def insert_property(entity_id: str, properties: dict, graph_id: str = 'default', api_base: str = API_DEFAULT):
    """新增实体属性"""
    return insert(f'{api_base}/{graph_id}/graph/entity/{entity_id}/properties', properties, name='entity_property')


def insert_ontology(object_: dict or list, object_type: str = 'entity', graph_id: str = 'default', api_base: str = API_DEFAULT):
    """新增本体对象"""
    return insert(f'{api_base}/ontology/{object_type}', object_, name=f'ontology_{object_type}')


def delete_ontology(object_id: str, object_type: str = 'entity', graph_id: str = 'default', api_base: str = API_DEFAULT):
    """删除本体对象"""
    url = f'{api_base}/ontology/{object_type}/{object_id}'
    res = requests.delete(url)
    if res.status_code == 200:
        print(f'ontology {object_type} deleted:', object_id)
        return True
    print(f'error', res.status_code, res.text)
    return False


if __name__ == '__main__':
    # print(search_entity('卫星星座', graph_id='123'))
    from smartetl.util.files import json_lines
    rows = []
    for row in json_lines('../../data/kg/graph-126/relations_trans.jsonl'):
        rows.append(row)
        if len(rows) >= 100:
            insert_graph(rows, 'relation', graph_id='126')
            break
