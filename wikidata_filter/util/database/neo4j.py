
from .base import Database

try:
    from neo4j import GraphDatabase
except:
    print('install neo4j first!')
    raise "neo4j not installed"


def node_to_dict(node):
    """将Node对象转换为字典"""
    return {
        "id": node.id,
        "labels": list(node.labels),
        "properties": dict(node.items())
    }


def relation_to_dict(rel):
    """将Relation对象转换为字典"""
    return {
        "id": rel.id,
        "labels": rel.type,
        "start_node": rel.start_node.id,
        "end_node": rel.end_node.id,
        "properties": dict(rel.items())
    }


class Neo4j(Database):
    def __init__(self, host: str = 'bolt://localhost:7687',
                 username: str = 'neo4j',
                 password: str = 'neo4j', **kwargs):
        auth = (username, password) if password else None
        self.client = GraphDatabase.driver(host, auth=auth)

    def scroll(self, label: str = None,
               cypher: str = None,
               where: dict = None,
               fetch_size: str = None,
               batch_size: int = 1000, **kwargs):
        skip = 0
        params = {
            'limit': batch_size
        }
        if not cypher:
            if where:
                cypher = f"MATCH (n:{label} $where) SKIP $skip LIMIT $limit return n"
                params['where'] = where
            else:
                cypher = f"MATCH (n:{label}) SKIP $skip LIMIT $limit return n"
        while True:
            params['skip'] = skip
            batch_counter = 0
            with self.client.session() as session:
                result = session.run(cypher, parameters=params)
                for record in result:
                    record_dict = {}
                    for key, value in record.items():
                        if hasattr(value, 'id'):  # 判断是否是Node或Relation
                            record_dict[key] = relation_to_dict(value) if hasattr(value, 'type') else node_to_dict(value)
                        else:
                            record_dict[key] = value
                    yield record_dict
                    batch_counter += 1
            if batch_counter < batch_size:
                break
            skip += batch_size

    def upsert(self, items: dict or list, **kwargs):
        if isinstance(items, dict):
            items = [items]
        to_insert = [item for item in items if 'id' in item]
        to_update = [item for item in items if 'id' not in item]
        if to_insert:
            with self.client.session() as session:
                for node in to_insert:
                    props = node.get('properties', {})
                    if props:
                        cypher = f'''CREATE (n:{' '.join(node['labels'])} $props)'''
                        session.run(cypher, parameters={'props': props})
                    else:
                        cypher = f'''CREATE (n:{' '.join(node['labels'])})'''
                        session.run(cypher)
        if to_update:
            with self.client.session() as session:
                for node in to_update:
                    cypher = f'''MATCH (n) where id(n)={node['id']} set n=$props'''
                    session.run(cypher, parameters={'props': node.get('properties', {})})
        return {'updated_count': len(to_update), 'inserted_count': len(to_insert)}
